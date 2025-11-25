package main

import (
	"bufio"
	"context"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	tsql "github.com/SimonWaldherr/tinySQL"
	"github.com/SimonWaldherr/tinySQL/internal/engine"
	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// ============================================================================
// SQL Beautifier
// ============================================================================

// BeautifyOptions configures SQL formatting behavior.
type BeautifyOptions struct {
	IndentString   string // Indentation characters (default: "  ")
	Uppercase      bool   // Convert keywords to uppercase
	LineWidth      int    // Max line width before wrapping (0 = no limit)
	NewlineOnComma bool   // Put each column on new line in SELECT
}

// DefaultBeautifyOptions returns standard formatting options.
func DefaultBeautifyOptions() BeautifyOptions {
	return BeautifyOptions{
		IndentString:   "  ",
		Uppercase:      true,
		LineWidth:      80,
		NewlineOnComma: false,
	}
}

// SQLBeautifier formats SQL statements for readability.
type SQLBeautifier struct {
	opts BeautifyOptions
}

// NewSQLBeautifier creates a beautifier with the given options.
func NewSQLBeautifier(opts BeautifyOptions) *SQLBeautifier {
	return &SQLBeautifier{opts: opts}
}

// Beautify formats a SQL statement.
func (b *SQLBeautifier) Beautify(sql string) string {
	sql = normalizeWhitespace(sql)
	tokens := tokenizeSQL(sql)
	if b.opts.Uppercase {
		tokens = uppercaseKeywords(tokens)
	}
	return b.formatTokens(tokens)
}

// SQL keywords that should start on a new line
var majorKeywords = map[string]bool{
	"SELECT": true, "FROM": true, "WHERE": true, "AND": true, "OR": true,
	"ORDER": true, "GROUP": true, "HAVING": true, "LIMIT": true, "OFFSET": true,
	"JOIN": true, "LEFT": true, "RIGHT": true, "INNER": true, "OUTER": true,
	"ON": true, "UNION": true, "EXCEPT": true, "INTERSECT": true,
	"INSERT": true, "INTO": true, "VALUES": true,
	"UPDATE": true, "SET": true, "DELETE": true,
	"CREATE": true, "ALTER": true, "DROP": true, "TABLE": true,
	"WITH": true, "AS": true, "CASE": true, "WHEN": true, "THEN": true, "ELSE": true, "END": true,
}

var allKeywords = map[string]bool{
	"SELECT": true, "DISTINCT": true, "FROM": true, "WHERE": true,
	"AND": true, "OR": true, "NOT": true, "IN": true, "LIKE": true,
	"BETWEEN": true, "IS": true, "NULL": true, "TRUE": true, "FALSE": true,
	"ORDER": true, "BY": true, "ASC": true, "DESC": true,
	"GROUP": true, "HAVING": true, "LIMIT": true, "OFFSET": true,
	"JOIN": true, "LEFT": true, "RIGHT": true, "INNER": true, "OUTER": true, "ON": true,
	"UNION": true, "ALL": true, "EXCEPT": true, "INTERSECT": true,
	"INSERT": true, "INTO": true, "VALUES": true,
	"UPDATE": true, "SET": true, "DELETE": true,
	"CREATE": true, "ALTER": true, "DROP": true, "TABLE": true, "INDEX": true, "VIEW": true,
	"PRIMARY": true, "KEY": true, "FOREIGN": true, "REFERENCES": true, "UNIQUE": true,
	"INT": true, "TEXT": true, "FLOAT": true, "BOOL": true, "JSON": true, "DATETIME": true,
	"WITH": true, "AS": true, "CASE": true, "WHEN": true, "THEN": true, "ELSE": true, "END": true,
	"COUNT": true, "SUM": true, "AVG": true, "MIN": true, "MAX": true,
	"COALESCE": true, "NULLIF": true, "CAST": true,
}

func normalizeWhitespace(s string) string {
	re := regexp.MustCompile(`\s+`)
	return strings.TrimSpace(re.ReplaceAllString(s, " "))
}

type sqlToken struct {
	typ   string
	value string
}

func tokenizeSQL(sql string) []sqlToken {
	var tokens []sqlToken
	i := 0
	for i < len(sql) {
		if sql[i] == ' ' || sql[i] == '\t' || sql[i] == '\n' {
			i++
			continue
		}

		if i+1 < len(sql) && sql[i] == '-' && sql[i+1] == '-' {
			j := i
			for j < len(sql) && sql[j] != '\n' {
				j++
			}
			tokens = append(tokens, sqlToken{"comment", sql[i:j]})
			i = j
			continue
		}

		if i+1 < len(sql) && sql[i] == '/' && sql[i+1] == '*' {
			j := i + 2
			for j+1 < len(sql) && !(sql[j] == '*' && sql[j+1] == '/') {
				j++
			}
			if j+1 < len(sql) {
				j += 2
			}
			tokens = append(tokens, sqlToken{"comment", sql[i:j]})
			i = j
			continue
		}

		if sql[i] == '\'' {
			j := i + 1
			for j < len(sql) && sql[j] != '\'' {
				if sql[j] == '\\' && j+1 < len(sql) {
					j++
				}
				j++
			}
			if j < len(sql) {
				j++
			}
			tokens = append(tokens, sqlToken{"string", sql[i:j]})
			i = j
			continue
		}

		if sql[i] >= '0' && sql[i] <= '9' {
			j := i
			for j < len(sql) && (sql[j] >= '0' && sql[j] <= '9' || sql[j] == '.') {
				j++
			}
			tokens = append(tokens, sqlToken{"number", sql[i:j]})
			i = j
			continue
		}

		if isIdentStart(sql[i]) {
			j := i
			for j < len(sql) && isIdentChar(sql[j]) {
				j++
			}
			word := sql[i:j]
			if allKeywords[strings.ToUpper(word)] {
				tokens = append(tokens, sqlToken{"keyword", word})
			} else {
				tokens = append(tokens, sqlToken{"ident", word})
			}
			i = j
			continue
		}

		tokens = append(tokens, sqlToken{"symbol", string(sql[i])})
		i++
	}
	return tokens
}

func isIdentStart(c byte) bool {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_'
}

func isIdentChar(c byte) bool {
	return isIdentStart(c) || (c >= '0' && c <= '9') || c == '.'
}

func uppercaseKeywords(tokens []sqlToken) []sqlToken {
	for i := range tokens {
		if tokens[i].typ == "keyword" {
			tokens[i].value = strings.ToUpper(tokens[i].value)
		}
	}
	return tokens
}

func (b *SQLBeautifier) formatTokens(tokens []sqlToken) string {
	var sb strings.Builder
	indent := 0
	newLine := true

	for i, tok := range tokens {
		upper := strings.ToUpper(tok.value)

		if tok.typ == "keyword" && majorKeywords[upper] {
			switch upper {
			case "SELECT", "INSERT", "UPDATE", "DELETE", "CREATE", "ALTER", "DROP", "WITH":
				if i > 0 {
					sb.WriteString("\n")
				}
				newLine = true
			case "FROM", "WHERE", "ORDER", "GROUP", "HAVING", "LIMIT", "OFFSET", "SET", "VALUES":
				sb.WriteString("\n")
				newLine = true
			case "AND", "OR":
				sb.WriteString("\n")
				indent = 1
				newLine = true
			case "JOIN", "LEFT", "RIGHT", "INNER":
				sb.WriteString("\n")
				newLine = true
			case "UNION", "EXCEPT", "INTERSECT":
				sb.WriteString("\n\n")
				indent = 0
				newLine = true
			}
		}

		if tok.value == "(" {
			indent++
		} else if tok.value == ")" {
			indent--
			if indent < 0 {
				indent = 0
			}
		}

		if newLine && tok.value != "(" && tok.value != ")" {
			for j := 0; j < indent; j++ {
				sb.WriteString(b.opts.IndentString)
			}
			newLine = false
		}

		sb.WriteString(tok.value)

		if i+1 < len(tokens) {
			next := tokens[i+1]
			if tok.value != "(" && next.value != ")" && next.value != "," && tok.value != "." && next.value != "." {
				sb.WriteString(" ")
			}
		}
	}

	return strings.TrimSpace(sb.String())
}

// ============================================================================
// Schema Browser
// ============================================================================

// SchemaBrowser provides schema inspection capabilities.
type SchemaBrowser struct {
	db *tsql.DB
}

// NewSchemaBrowser creates a schema browser for the given database.
func NewSchemaBrowser(db *tsql.DB) *SchemaBrowser {
	return &SchemaBrowser{db: db}
}

// ListTables returns all table names in a tenant.
func (sb *SchemaBrowser) ListTables(tenant string) []string {
	tables := sb.db.ListTables(tenant)
	names := make([]string, len(tables))
	for i, t := range tables {
		names[i] = t.Name
	}
	sort.Strings(names)
	return names
}

// TableInfo holds table metadata.
type TableInfo struct {
	Name     string
	Columns  []ColumnInfo
	RowCount int
}

// ColumnInfo holds column metadata.
type ColumnInfo struct {
	Name     string
	Type     string
	Nullable bool
	Primary  bool
}

// DescribeTable returns detailed info about a table.
func (sb *SchemaBrowser) DescribeTable(tenant, table string) (*TableInfo, error) {
	t, err := sb.db.Get(tenant, table)
	if err != nil || t == nil {
		return nil, fmt.Errorf("table %q not found in tenant %q", table, tenant)
	}

	info := &TableInfo{
		Name:     table,
		RowCount: len(t.Rows),
	}

	for _, col := range t.Cols {
		info.Columns = append(info.Columns, ColumnInfo{
			Name:     col.Name,
			Type:     col.Type.String(),
			Nullable: col.Constraint != storage.PrimaryKey,
			Primary:  col.Constraint == storage.PrimaryKey,
		})
	}

	return info, nil
}

// PrintSchema prints a formatted schema listing.
func (sb *SchemaBrowser) PrintSchema(tenant string, w *tabwriter.Writer) {
	tables := sb.ListTables(tenant)
	fmt.Fprintf(w, "Schema: %s\n", tenant)
	fmt.Fprintf(w, "Tables: %d\n\n", len(tables))

	for _, table := range tables {
		info, err := sb.DescribeTable(tenant, table)
		if err != nil {
			continue
		}
		fmt.Fprintf(w, "TABLE %s (%d rows)\n", info.Name, info.RowCount)
		fmt.Fprintf(w, "  %-20s\t%-10s\t%s\n", "Column", "Type", "Flags")
		fmt.Fprintf(w, "  %-20s\t%-10s\t%s\n", "------", "----", "-----")
		for _, col := range info.Columns {
			flags := ""
			if col.Primary {
				flags += "PK "
			}
			if !col.Nullable {
				flags += "NOT NULL"
			}
			fmt.Fprintf(w, "  %-20s\t%-10s\t%s\n", col.Name, col.Type, flags)
		}
		fmt.Fprintln(w)
	}
	w.Flush()
}

// ============================================================================
// Query History
// ============================================================================

// QueryHistoryEntry records a single query execution.
type QueryHistoryEntry struct {
	ID        int
	SQL       string
	Timestamp time.Time
	Duration  time.Duration
	RowCount  int
	Error     string
}

// QueryHistory tracks executed queries.
type QueryHistory struct {
	entries []QueryHistoryEntry
	maxSize int
	nextID  int
}

// NewQueryHistory creates a history tracker with max entries.
func NewQueryHistory(maxSize int) *QueryHistory {
	return &QueryHistory{
		entries: make([]QueryHistoryEntry, 0, maxSize),
		maxSize: maxSize,
		nextID:  1,
	}
}

// Add records a new query execution.
func (qh *QueryHistory) Add(sql string, duration time.Duration, rowCount int, err error) {
	entry := QueryHistoryEntry{
		ID:        qh.nextID,
		SQL:       sql,
		Timestamp: time.Now(),
		Duration:  duration,
		RowCount:  rowCount,
	}
	if err != nil {
		entry.Error = err.Error()
	}
	qh.nextID++

	qh.entries = append(qh.entries, entry)
	if len(qh.entries) > qh.maxSize {
		qh.entries = qh.entries[1:]
	}
}

// Last returns the last n entries.
func (qh *QueryHistory) Last(n int) []QueryHistoryEntry {
	if n > len(qh.entries) {
		n = len(qh.entries)
	}
	start := len(qh.entries) - n
	result := make([]QueryHistoryEntry, n)
	copy(result, qh.entries[start:])
	return result
}

// Search finds entries matching a pattern.
func (qh *QueryHistory) Search(pattern string) []QueryHistoryEntry {
	var results []QueryHistoryEntry
	lower := strings.ToLower(pattern)
	for _, e := range qh.entries {
		if strings.Contains(strings.ToLower(e.SQL), lower) {
			results = append(results, e)
		}
	}
	return results
}

// PrintHistory displays query history in a formatted table.
func (qh *QueryHistory) PrintHistory(w *tabwriter.Writer, entries []QueryHistoryEntry) {
	fmt.Fprintf(w, "ID\tTime\tDuration\tRows\tStatus\tSQL\n")
	fmt.Fprintf(w, "--\t----\t--------\t----\t------\t---\n")
	for _, e := range entries {
		status := "OK"
		if e.Error != "" {
			status = "ERR"
		}
		sql := e.SQL
		if len(sql) > 50 {
			sql = sql[:47] + "..."
		}
		fmt.Fprintf(w, "%d\t%s\t%v\t%d\t%s\t%s\n",
			e.ID, e.Timestamp.Format("15:04:05"), e.Duration, e.RowCount, status, sql)
	}
	w.Flush()
}

// ============================================================================
// Query Explain
// ============================================================================

// QueryPlan represents a simple query execution plan.
type QueryPlan struct {
	Steps []PlanStep
}

// PlanStep is a single step in the query plan.
type PlanStep struct {
	Operation string
	Object    string
	Rows      string
	Cost      string
	Details   string
}

// ExplainQuery generates a simple query plan.
func ExplainQuery(sql string) (*QueryPlan, error) {
	stmt, err := tsql.ParseSQL(sql)
	if err != nil {
		return nil, err
	}

	plan := &QueryPlan{}

	switch s := stmt.(type) {
	case *engine.Select:
		if s.From.Table != "" {
			plan.Steps = append(plan.Steps, PlanStep{
				Operation: "TABLE SCAN",
				Object:    s.From.Table,
				Cost:      "low",
				Details:   "Sequential scan of table",
			})
		}

		for _, join := range s.Joins {
			joinTypeStr := "INNER"
			switch join.Type {
			case engine.JoinLeft:
				joinTypeStr = "LEFT"
			case engine.JoinRight:
				joinTypeStr = "RIGHT"
			}
			plan.Steps = append(plan.Steps, PlanStep{
				Operation: "NESTED LOOP JOIN",
				Object:    join.Right.Table,
				Cost:      "medium",
				Details:   fmt.Sprintf("%s join", joinTypeStr),
			})
		}

		if s.Where != nil {
			plan.Steps = append(plan.Steps, PlanStep{
				Operation: "FILTER",
				Object:    "-",
				Cost:      "low",
				Details:   "Apply WHERE conditions",
			})
		}

		if len(s.GroupBy) > 0 {
			plan.Steps = append(plan.Steps, PlanStep{
				Operation: "AGGREGATE",
				Object:    "-",
				Cost:      "medium",
				Details:   "Group and aggregate",
			})
		}

		if len(s.OrderBy) > 0 {
			plan.Steps = append(plan.Steps, PlanStep{
				Operation: "SORT",
				Object:    "-",
				Cost:      "medium-high",
				Details:   "Sort results",
			})
		}

		if s.Limit != nil || s.Offset != nil {
			plan.Steps = append(plan.Steps, PlanStep{
				Operation: "LIMIT/OFFSET",
				Object:    "-",
				Cost:      "low",
				Details:   "Apply row limits",
			})
		}

		plan.Steps = append(plan.Steps, PlanStep{
			Operation: "PROJECT",
			Object:    "-",
			Cost:      "low",
			Details:   fmt.Sprintf("Return %d columns", len(s.Projs)),
		})

	case *engine.Insert:
		plan.Steps = append(plan.Steps, PlanStep{
			Operation: "INSERT",
			Object:    s.Table,
			Cost:      "low",
			Details:   fmt.Sprintf("Insert %d row(s)", len(s.Rows)),
		})

	case *engine.Update:
		plan.Steps = append(plan.Steps, PlanStep{
			Operation: "TABLE SCAN",
			Object:    s.Table,
			Cost:      "low",
		})
		plan.Steps = append(plan.Steps, PlanStep{
			Operation: "UPDATE",
			Object:    s.Table,
			Cost:      "low",
			Details:   fmt.Sprintf("Update %d columns", len(s.Sets)),
		})

	case *engine.Delete:
		plan.Steps = append(plan.Steps, PlanStep{
			Operation: "TABLE SCAN",
			Object:    s.Table,
			Cost:      "low",
		})
		plan.Steps = append(plan.Steps, PlanStep{
			Operation: "DELETE",
			Object:    s.Table,
			Cost:      "low",
		})
	}

	return plan, nil
}

// PrintPlan displays a query plan.
func PrintPlan(plan *QueryPlan, w *tabwriter.Writer) {
	fmt.Fprintf(w, "Step\tOperation\tObject\tCost\tDetails\n")
	fmt.Fprintf(w, "----\t---------\t------\t----\t-------\n")
	for i, step := range plan.Steps {
		fmt.Fprintf(w, "%d\t%s\t%s\t%s\t%s\n",
			i+1, step.Operation, step.Object, step.Cost, step.Details)
	}
	w.Flush()
}

// ============================================================================
// Data Export
// ============================================================================

// ExportFormat specifies output format.
type ExportFormat string

const (
	FormatCSV  ExportFormat = "csv"
	FormatJSON ExportFormat = "json"
	FormatSQL  ExportFormat = "sql"
)

// Exporter exports query results to various formats.
type Exporter struct {
	format ExportFormat
}

// NewExporter creates an exporter for the given format.
func NewExporter(format ExportFormat) *Exporter {
	return &Exporter{format: format}
}

// Export writes result set to writer.
func (e *Exporter) Export(rs *tsql.ResultSet, tableName string, w *os.File) error {
	switch e.format {
	case FormatCSV:
		return e.exportCSV(rs, w)
	case FormatJSON:
		return e.exportJSON(rs, w)
	case FormatSQL:
		return e.exportSQL(rs, tableName, w)
	default:
		return fmt.Errorf("unknown format: %s", e.format)
	}
}

func (e *Exporter) exportCSV(rs *tsql.ResultSet, w *os.File) error {
	writer := csv.NewWriter(w)
	defer writer.Flush()

	if err := writer.Write(rs.Cols); err != nil {
		return err
	}

	for _, row := range rs.Rows {
		record := make([]string, len(rs.Cols))
		for i, col := range rs.Cols {
			if v, ok := row[strings.ToLower(col)]; ok && v != nil {
				record[i] = fmt.Sprintf("%v", v)
			}
		}
		if err := writer.Write(record); err != nil {
			return err
		}
	}
	return nil
}

func (e *Exporter) exportJSON(rs *tsql.ResultSet, w *os.File) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")

	var data []map[string]any
	for _, row := range rs.Rows {
		item := make(map[string]any)
		for _, col := range rs.Cols {
			item[col] = row[strings.ToLower(col)]
		}
		data = append(data, item)
	}
	return enc.Encode(data)
}

func (e *Exporter) exportSQL(rs *tsql.ResultSet, tableName string, w *os.File) error {
	for _, row := range rs.Rows {
		var values []string
		for _, col := range rs.Cols {
			v := row[strings.ToLower(col)]
			if v == nil {
				values = append(values, "NULL")
			} else if s, ok := v.(string); ok {
				values = append(values, fmt.Sprintf("'%s'", strings.ReplaceAll(s, "'", "''")))
			} else {
				values = append(values, fmt.Sprintf("%v", v))
			}
		}
		fmt.Fprintf(w, "INSERT INTO %s (%s) VALUES (%s);\n",
			tableName, strings.Join(rs.Cols, ", "), strings.Join(values, ", "))
	}
	return nil
}

// ============================================================================
// Query Templates
// ============================================================================

// QueryTemplate is a reusable query snippet.
type QueryTemplate struct {
	Name        string
	Description string
	SQL         string
	Parameters  []string
}

// CommonTemplates returns built-in query templates.
func CommonTemplates() []QueryTemplate {
	return []QueryTemplate{
		{
			Name:        "select_all",
			Description: "Select all rows from a table",
			SQL:         "SELECT * FROM {table}",
			Parameters:  []string{"table"},
		},
		{
			Name:        "select_top",
			Description: "Select top N rows from a table",
			SQL:         "SELECT * FROM {table} LIMIT {limit}",
			Parameters:  []string{"table", "limit"},
		},
		{
			Name:        "select_where",
			Description: "Select with WHERE condition",
			SQL:         "SELECT * FROM {table} WHERE {column} = {value}",
			Parameters:  []string{"table", "column", "value"},
		},
		{
			Name:        "count_rows",
			Description: "Count rows in a table",
			SQL:         "SELECT COUNT(*) AS cnt FROM {table}",
			Parameters:  []string{"table"},
		},
		{
			Name:        "group_count",
			Description: "Count rows grouped by column",
			SQL:         "SELECT {column}, COUNT(*) AS cnt FROM {table} GROUP BY {column} ORDER BY cnt DESC",
			Parameters:  []string{"table", "column"},
		},
		{
			Name:        "distinct_values",
			Description: "Get distinct values in a column",
			SQL:         "SELECT DISTINCT {column} FROM {table} ORDER BY {column}",
			Parameters:  []string{"table", "column"},
		},
		{
			Name:        "join_tables",
			Description: "Join two tables",
			SQL:         "SELECT * FROM {table1} t1 JOIN {table2} t2 ON t1.{key1} = t2.{key2}",
			Parameters:  []string{"table1", "table2", "key1", "key2"},
		},
		{
			Name:        "insert_row",
			Description: "Insert a single row",
			SQL:         "INSERT INTO {table} ({columns}) VALUES ({values})",
			Parameters:  []string{"table", "columns", "values"},
		},
		{
			Name:        "update_row",
			Description: "Update rows matching condition",
			SQL:         "UPDATE {table} SET {column} = {value} WHERE {condition}",
			Parameters:  []string{"table", "column", "value", "condition"},
		},
		{
			Name:        "delete_rows",
			Description: "Delete rows matching condition",
			SQL:         "DELETE FROM {table} WHERE {condition}",
			Parameters:  []string{"table", "condition"},
		},
	}
}

// ApplyTemplate substitutes parameters into a template.
func ApplyTemplate(tmpl QueryTemplate, params map[string]string) string {
	result := tmpl.SQL
	for k, v := range params {
		result = strings.ReplaceAll(result, "{"+k+"}", v)
	}
	return result
}

// ============================================================================
// SQL Validator
// ============================================================================

// ValidationResult holds the result of SQL validation.
type ValidationResult struct {
	Valid    bool
	Error    string
	Warnings []string
	SQLType  string
}

// ValidateSQL checks if SQL is syntactically correct.
func ValidateSQL(sql string) ValidationResult {
	result := ValidationResult{Valid: true}

	stmt, err := tsql.ParseSQL(sql)
	if err != nil {
		result.Valid = false
		result.Error = err.Error()
		return result
	}

	switch stmt.(type) {
	case *engine.Select:
		result.SQLType = "SELECT"
	case *engine.Insert:
		result.SQLType = "INSERT"
	case *engine.Update:
		result.SQLType = "UPDATE"
	case *engine.Delete:
		result.SQLType = "DELETE"
	case *engine.CreateTable:
		result.SQLType = "CREATE TABLE"
	case *engine.DropTable:
		result.SQLType = "DROP TABLE"
	default:
		result.SQLType = "UNKNOWN"
	}

	upperSQL := strings.ToUpper(sql)

	if strings.Contains(upperSQL, "SELECT *") {
		result.Warnings = append(result.Warnings, "Consider specifying columns instead of SELECT *")
	}

	if strings.Contains(upperSQL, "DELETE FROM") && !strings.Contains(upperSQL, "WHERE") {
		result.Warnings = append(result.Warnings, "DELETE without WHERE will delete all rows!")
	}

	if strings.Contains(upperSQL, "UPDATE") && !strings.Contains(upperSQL, "WHERE") {
		result.Warnings = append(result.Warnings, "UPDATE without WHERE will update all rows!")
	}

	return result
}

// ============================================================================
// Interactive SQL Tools REPL
// ============================================================================

func main() {
	beautifyCmd := flag.NewFlagSet("beautify", flag.ExitOnError)
	beautifyUpper := beautifyCmd.Bool("upper", true, "Uppercase keywords")

	validateCmd := flag.NewFlagSet("validate", flag.ExitOnError)

	explainCmd := flag.NewFlagSet("explain", flag.ExitOnError)

	replCmd := flag.NewFlagSet("repl", flag.ExitOnError)
	replTenant := replCmd.String("tenant", "default", "Tenant name")

	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	switch os.Args[1] {
	case "beautify":
		beautifyCmd.Parse(os.Args[2:])
		if beautifyCmd.NArg() < 1 {
			fmt.Println("Usage: sqltools beautify [-upper=true] <sql>")
			os.Exit(1)
		}
		sql := strings.Join(beautifyCmd.Args(), " ")
		opts := DefaultBeautifyOptions()
		opts.Uppercase = *beautifyUpper
		b := NewSQLBeautifier(opts)
		fmt.Println(b.Beautify(sql))

	case "validate":
		validateCmd.Parse(os.Args[2:])
		if validateCmd.NArg() < 1 {
			fmt.Println("Usage: sqltools validate <sql>")
			os.Exit(1)
		}
		sql := strings.Join(validateCmd.Args(), " ")
		result := ValidateSQL(sql)
		if result.Valid {
			fmt.Printf("✓ Valid %s statement\n", result.SQLType)
			for _, w := range result.Warnings {
				fmt.Printf("⚠ Warning: %s\n", w)
			}
		} else {
			fmt.Printf("✗ Invalid SQL: %s\n", result.Error)
			os.Exit(1)
		}

	case "explain":
		explainCmd.Parse(os.Args[2:])
		if explainCmd.NArg() < 1 {
			fmt.Println("Usage: sqltools explain <sql>")
			os.Exit(1)
		}
		sql := strings.Join(explainCmd.Args(), " ")
		plan, err := ExplainQuery(sql)
		if err != nil {
			fmt.Printf("Error: %v\n", err)
			os.Exit(1)
		}
		w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
		PrintPlan(plan, w)

	case "templates":
		fmt.Println("Available Query Templates:")
		fmt.Println("==========================")
		for _, t := range CommonTemplates() {
			fmt.Printf("\n%s - %s\n", t.Name, t.Description)
			fmt.Printf("  SQL: %s\n", t.SQL)
			fmt.Printf("  Parameters: %s\n", strings.Join(t.Parameters, ", "))
		}

	case "repl":
		replCmd.Parse(os.Args[2:])
		runToolsREPL(*replTenant)

	default:
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Println(`SQL Tools - SSMS-like features for tinySQL

Commands:
  beautify [-upper=true] <sql>   Format SQL statement
  validate <sql>                 Check SQL syntax
  explain <sql>                  Show query execution plan
  templates                      List query templates
  repl [-tenant=default]         Interactive SQL tools shell

Examples:
  sqltools beautify "select * from users where id=1"
  sqltools validate "SELECT name FROM users"
  sqltools explain "SELECT * FROM orders JOIN users ON orders.user_id = users.id"
  sqltools repl`)
}

func runToolsREPL(tenant string) {
	db := tsql.NewDB()
	ctx := context.Background()
	history := NewQueryHistory(100)
	beautifier := NewSQLBeautifier(DefaultBeautifyOptions())
	browser := NewSchemaBrowser(db)

	fmt.Println("SQL Tools REPL")
	fmt.Println("Type .help for commands, .quit to exit")
	fmt.Println()

	scanner := bufio.NewScanner(os.Stdin)
	var sqlBuf strings.Builder

	for {
		if sqlBuf.Len() == 0 {
			fmt.Print("sql> ")
		} else {
			fmt.Print(" ... ")
		}

		if !scanner.Scan() {
			break
		}

		line := strings.TrimSpace(scanner.Text())

		if sqlBuf.Len() == 0 && strings.HasPrefix(line, ".") {
			handleToolsCommand(line, db, tenant, history, beautifier, browser)
			continue
		}

		sqlBuf.WriteString(line)

		if strings.HasSuffix(line, ";") {
			sql := strings.TrimSuffix(strings.TrimSpace(sqlBuf.String()), ";")
			sqlBuf.Reset()

			if sql == "" {
				continue
			}

			start := time.Now()
			stmt, err := tsql.ParseSQL(sql)
			if err != nil {
				history.Add(sql, time.Since(start), 0, err)
				fmt.Printf("Parse error: %v\n", err)
				continue
			}

			rs, err := tsql.Execute(ctx, db, tenant, stmt)
			duration := time.Since(start)

			if err != nil {
				history.Add(sql, duration, 0, err)
				fmt.Printf("Error: %v\n", err)
				continue
			}

			history.Add(sql, duration, len(rs.Rows), nil)

			if len(rs.Rows) > 0 {
				w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
				fmt.Fprintln(w, strings.Join(rs.Cols, "\t"))
				for _, row := range rs.Rows {
					var vals []string
					for _, col := range rs.Cols {
						v := row[strings.ToLower(col)]
						if v == nil {
							vals = append(vals, "NULL")
						} else {
							vals = append(vals, fmt.Sprintf("%v", v))
						}
					}
					fmt.Fprintln(w, strings.Join(vals, "\t"))
				}
				w.Flush()
			}
			fmt.Printf("(%d rows, %v)\n", len(rs.Rows), duration)
		} else {
			sqlBuf.WriteString(" ")
		}
	}
}

func handleToolsCommand(cmd string, db *tsql.DB, tenant string, history *QueryHistory, beautifier *SQLBeautifier, browser *SchemaBrowser) {
	parts := strings.Fields(cmd)
	if len(parts) == 0 {
		return
	}

	switch parts[0] {
	case ".help":
		fmt.Println(`
Commands:
  .help                 Show this help
  .quit                 Exit
  .tables               List tables
  .schema [table]       Show schema (all tables or specific table)
  .history [n]          Show last n queries (default 10)
  .beautify <sql>       Format SQL
  .validate <sql>       Check SQL syntax
  .explain <sql>        Show query plan
  .export <format> <file> <sql>   Export results (csv, json, sql)
  .template <name>      Show template
  .templates            List all templates`)

	case ".quit", ".exit":
		os.Exit(0)

	case ".tables":
		tables := browser.ListTables(tenant)
		for _, t := range tables {
			fmt.Println(t)
		}

	case ".schema":
		w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
		if len(parts) > 1 {
			info, err := browser.DescribeTable(tenant, parts[1])
			if err != nil {
				fmt.Println("Error:", err)
				return
			}
			fmt.Printf("Table: %s (%d rows)\n", info.Name, info.RowCount)
			fmt.Fprintf(w, "Column\tType\tFlags\n")
			fmt.Fprintf(w, "------\t----\t-----\n")
			for _, col := range info.Columns {
				flags := ""
				if col.Primary {
					flags += "PK "
				}
				if !col.Nullable {
					flags += "NOT NULL"
				}
				fmt.Fprintf(w, "%s\t%s\t%s\n", col.Name, col.Type, flags)
			}
			w.Flush()
		} else {
			browser.PrintSchema(tenant, w)
		}

	case ".history":
		n := 10
		if len(parts) > 1 {
			fmt.Sscanf(parts[1], "%d", &n)
		}
		w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
		history.PrintHistory(w, history.Last(n))

	case ".beautify":
		if len(parts) < 2 {
			fmt.Println("Usage: .beautify <sql>")
			return
		}
		sql := strings.Join(parts[1:], " ")
		fmt.Println(beautifier.Beautify(sql))

	case ".validate":
		if len(parts) < 2 {
			fmt.Println("Usage: .validate <sql>")
			return
		}
		sql := strings.Join(parts[1:], " ")
		result := ValidateSQL(sql)
		if result.Valid {
			fmt.Printf("✓ Valid %s statement\n", result.SQLType)
			for _, w := range result.Warnings {
				fmt.Printf("⚠ %s\n", w)
			}
		} else {
			fmt.Printf("✗ Invalid: %s\n", result.Error)
		}

	case ".explain":
		if len(parts) < 2 {
			fmt.Println("Usage: .explain <sql>")
			return
		}
		sql := strings.Join(parts[1:], " ")
		plan, err := ExplainQuery(sql)
		if err != nil {
			fmt.Println("Error:", err)
			return
		}
		w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
		PrintPlan(plan, w)

	case ".templates":
		for _, t := range CommonTemplates() {
			fmt.Printf("  %s - %s\n", t.Name, t.Description)
		}

	case ".template":
		if len(parts) < 2 {
			fmt.Println("Usage: .template <name>")
			return
		}
		name := parts[1]
		for _, t := range CommonTemplates() {
			if t.Name == name {
				fmt.Printf("Name: %s\n", t.Name)
				fmt.Printf("Description: %s\n", t.Description)
				fmt.Printf("SQL: %s\n", t.SQL)
				fmt.Printf("Parameters: %s\n", strings.Join(t.Parameters, ", "))
				return
			}
		}
		fmt.Printf("Template %q not found\n", name)

	default:
		fmt.Printf("Unknown command: %s (type .help for commands)\n", parts[0])
	}
}
