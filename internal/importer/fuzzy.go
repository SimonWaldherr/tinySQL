package importer

import (
	"bufio"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"regexp"
	"strconv"
	"strings"
	"unicode"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// FuzzyImportOptions extends ImportOptions with fuzzy parsing capabilities
type FuzzyImportOptions struct {
	*ImportOptions
	
	// SkipInvalidRows skips rows that don't match the expected column count (default true)
	SkipInvalidRows bool
	
	// TrimWhitespace aggressively trims whitespace from all values (default true)
	TrimWhitespace bool
	
	// FixQuotes attempts to fix unmatched quotes in CSV data (default true)
	FixQuotes bool
	
	// CoerceTypes attempts to coerce invalid values to the expected type (default true)
	CoerceTypes bool
	
	// MaxSkippedRows maximum number of rows to skip before giving up (default 100)
	MaxSkippedRows int
	
	// AllowMixedTypes allows columns to have mixed types (converts to TEXT) (default true)
	AllowMixedTypes bool
	
	// FuzzyJSON attempts to parse malformed JSON (default true)
	FuzzyJSON bool
	
	// RemoveInvalidChars removes invalid UTF-8 characters (default true)
	RemoveInvalidChars bool
	
	// AutoFixDelimiters tries to detect and fix inconsistent delimiters (default true)
	AutoFixDelimiters bool
}

// FuzzyImportCSV is a more forgiving version of ImportCSV that handles malformed data
func FuzzyImportCSV(
	ctx context.Context,
	db *storage.DB,
	tenant string,
	tableName string,
	src io.Reader,
	opts *FuzzyImportOptions,
) (*ImportResult, error) {
	if opts == nil {
		opts = &FuzzyImportOptions{
			ImportOptions: &ImportOptions{},
		}
	}
	
	applyFuzzyDefaults(opts)
	
	if opts.ImportOptions == nil {
		opts.ImportOptions = &ImportOptions{}
	}
	applyDefaults(opts.ImportOptions)
	
	if opts.TableName != "" {
		tableName = opts.TableName
	}
	if tableName == "" {
		return nil, fmt.Errorf("table name is required")
	}
	
	result := &ImportResult{
		Errors: make([]string, 0),
	}
	
	// Read and clean the input data
	cleanedData, err := cleanInputData(src, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to clean input data: %v", err)
	}
	
	// Detect delimiter and structure
	delimiter, records, err := fuzzyParseCSV(cleanedData, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to parse CSV: %v", err)
	}
	
	result.Delimiter = delimiter
	
	if len(records) == 0 {
		return result, nil
	}
	
	// Determine if first row is header
	headerMode := "auto"
	if opts.ImportOptions != nil && opts.ImportOptions.HeaderMode != "" {
		headerMode = opts.ImportOptions.HeaderMode
	}
	hasHeader := fuzzyDecideHeader(records, headerMode)
	result.HadHeader = hasHeader
	
	var headers []string
	var dataRecords [][]string
	
	if hasHeader {
		headers = sanitizeColumnNames(records[0])
		dataRecords = records[1:]
	} else {
		numCols := len(records[0])
		headers = generateColumnNames(numCols)
		dataRecords = records
	}
	
	numCols := len(headers)
	
	// Normalize all records to have the same number of columns
	dataRecords = normalizeRecords(dataRecords, numCols, opts)
	
	// Infer column types
	var columnTypes []storage.ColType
	typeInference := true
	if opts.ImportOptions != nil {
		typeInference = opts.ImportOptions.TypeInference
	}
	if typeInference {
		columnTypes = fuzzyInferColumnTypes(dataRecords, numCols, opts)
	} else {
		columnTypes = make([]storage.ColType, numCols)
		for i := range columnTypes {
			columnTypes[i] = storage.TextType
		}
	}
	
	result.ColumnNames = headers
	result.ColumnTypes = columnTypes
	
	// Create table if needed
	createTable := true
	truncate := false
	if opts.ImportOptions != nil {
		createTable = opts.ImportOptions.CreateTable
		truncate = opts.ImportOptions.Truncate
	}
	if createTable {
		columns := make([]storage.Column, numCols)
		for i := 0; i < numCols; i++ {
			columns[i] = storage.Column{
				Name: headers[i],
				Type: columnTypes[i],
			}
		}
		
		table := storage.NewTable(tableName, columns, false)
		if err := db.Put(tenant, table); err != nil {
			return nil, fmt.Errorf("failed to create table: %v", err)
		}
		
		if truncate {
			table.Rows = nil
		}
	}
	
	// Get the table
	table, err := db.Get(tenant, tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to get table: %v", err)
	}
	
	// Insert data with fuzzy conversion
	skippedRows := 0
	for rowIdx, record := range dataRecords {
		if len(record) != numCols {
			skippedRows++
			result.RowsSkipped++
			result.Errors = append(result.Errors, 
				fmt.Sprintf("row %d: column count mismatch (expected %d, got %d)", 
					rowIdx+1, numCols, len(record)))
			
			if skippedRows > opts.MaxSkippedRows {
				return nil, fmt.Errorf("too many skipped rows (%d), aborting import", skippedRows)
			}
			continue
		}
		
		row := make([]interface{}, numCols)
		hasError := false
		
		for colIdx := 0; colIdx < numCols; colIdx++ {
			value := record[colIdx]
			if opts.TrimWhitespace {
				value = strings.TrimSpace(value)
			}
			
			converted, err := fuzzyConvertValue(value, columnTypes[colIdx], opts)
			if err != nil {
				if !opts.CoerceTypes {
					hasError = true
					result.Errors = append(result.Errors,
						fmt.Sprintf("row %d, col %s: %v", rowIdx+1, headers[colIdx], err))
					break
				}
				// Fallback to string
				converted = value
			}
			
			row[colIdx] = converted
		}
		
		if hasError && !opts.SkipInvalidRows {
			return nil, fmt.Errorf("import failed at row %d", rowIdx+1)
		}
		
		if !hasError {
			table.Rows = append(table.Rows, row)
			result.RowsInserted++
		} else {
			result.RowsSkipped++
		}
	}
	
	return result, nil
}

// fuzzyDecideHeader is a more lenient header detection that also checks for
// typical header patterns like lowercase/capitalized names
func fuzzyDecideHeader(records [][]string, mode string) bool {
	switch strings.ToLower(strings.TrimSpace(mode)) {
	case "present":
		return true
	case "absent":
		return false
	}

	if len(records) < 2 {
		return false
	}

	first := records[0]
	body := records[1:]
	cols := len(first)
	headerish := 0

	for c := 0; c < cols; c++ {
		headVal := strings.TrimSpace(first[c])
		
		// Check if header looks like a typical column name
		isTypicalHeader := false
		
		// Empty or very long is not a good header
		if len(headVal) == 0 || len(headVal) > 50 {
			continue
		}
		
		// Check if it's a single word or snake_case/camelCase identifier
		matched, _ := regexp.MatchString(`^[a-zA-Z][a-zA-Z0-9_]*$`, headVal)
		if matched {
			isTypicalHeader = true
		}
		
		// Check if first row is NOT numeric but data IS
		headNum := looksNumeric(headVal)
		dataNum := 0
		rows := 0
		for _, r := range body {
			if c >= len(r) {
				continue
			}
			if looksNumeric(r[c]) {
				dataNum++
			}
			rows++
		}
		
		// Header-like if: (typical name pattern) OR (non-numeric header with >60% numeric data)
		if isTypicalHeader {
			headerish++
		} else if rows > 0 && !headNum && float64(dataNum)/float64(rows) > 0.6 {
			headerish++
		}
	}
	
	return float64(headerish)/float64(cols) >= 0.5
}

// applyFuzzyDefaults sets default values for fuzzy import options
func applyFuzzyDefaults(opts *FuzzyImportOptions) {
	if opts.MaxSkippedRows == 0 {
		opts.MaxSkippedRows = 100
	}
	
	// Set defaults to true if not explicitly set
	opts.SkipInvalidRows = true
	opts.TrimWhitespace = true
	opts.FixQuotes = true
	opts.CoerceTypes = true
	opts.AllowMixedTypes = true
	opts.FuzzyJSON = true
	opts.RemoveInvalidChars = true
	opts.AutoFixDelimiters = true
}

// cleanInputData cleans and normalizes input data
func cleanInputData(src io.Reader, opts *FuzzyImportOptions) (string, error) {
	data, err := io.ReadAll(src)
	if err != nil {
		return "", err
	}
	
	str := string(data)
	
	// Remove BOM if present
	str = strings.TrimPrefix(str, "\ufeff")
	
	// Remove invalid UTF-8 characters
	if opts.RemoveInvalidChars {
		str = strings.ToValidUTF8(str, "")
	}
	
	// Normalize line endings
	str = strings.ReplaceAll(str, "\r\n", "\n")
	str = strings.ReplaceAll(str, "\r", "\n")
	
	// Remove trailing whitespace from lines
	lines := strings.Split(str, "\n")
	for i, line := range lines {
		lines[i] = strings.TrimRight(line, " \t")
	}
	str = strings.Join(lines, "\n")
	
	return str, nil
}

// fuzzyParseCSV attempts to parse CSV data with various strategies
func fuzzyParseCSV(data string, opts *FuzzyImportOptions) (rune, [][]string, error) {
	// Try standard delimiters
	delimiters := opts.DelimiterCandidates
	if len(delimiters) == 0 {
		delimiters = []rune{',', ';', '\t', '|'}
	}
	
	var bestDelimiter rune
	var bestRecords [][]string
	var bestScore float64
	
	for _, delim := range delimiters {
		records, score := tryParseWithDelimiter(data, delim, opts)
		if score > bestScore && len(records) > 0 {
			bestScore = score
			bestDelimiter = delim
			bestRecords = records
		}
	}
	
	if len(bestRecords) == 0 {
		return ',', nil, fmt.Errorf("failed to parse CSV with any delimiter")
	}
	
	return bestDelimiter, bestRecords, nil
}

// tryParseWithDelimiter attempts to parse CSV with a specific delimiter
func tryParseWithDelimiter(data string, delim rune, opts *FuzzyImportOptions) ([][]string, float64) {
	reader := csv.NewReader(strings.NewReader(data))
	reader.Comma = delim
	reader.LazyQuotes = true
	reader.TrimLeadingSpace = true
	reader.FieldsPerRecord = -1 // Allow variable fields
	
	var records [][]string
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			// Try to fix the record
			if opts.FixQuotes {
				continue // Skip problematic records
			}
			return nil, 0
		}
		records = append(records, record)
	}
	
	if len(records) == 0 {
		return nil, 0
	}
	
	// Score based on consistency of column counts
	columnCounts := make(map[int]int)
	for _, record := range records {
		columnCounts[len(record)]++
	}
	
	// Find most common column count
	maxCount := 0
	var mostCommonCols int
	for cols, count := range columnCounts {
		if count > maxCount {
			maxCount = count
			mostCommonCols = cols
		}
	}
	
	// Score is the percentage of rows with the most common column count
	score := float64(maxCount) / float64(len(records))
	
	// Bonus if most common column count is > 1
	if mostCommonCols > 1 {
		score *= 1.2
	}
	
	return records, score
}

// normalizeRecords ensures all records have the same number of columns
func normalizeRecords(records [][]string, expectedCols int, opts *FuzzyImportOptions) [][]string {
	normalized := make([][]string, 0, len(records))
	
	for _, record := range records {
		// Pad short records
		if len(record) < expectedCols {
			padded := make([]string, expectedCols)
			copy(padded, record)
			for i := len(record); i < expectedCols; i++ {
				padded[i] = ""
			}
			normalized = append(normalized, padded)
		} else if len(record) > expectedCols {
			// Truncate long records or skip
			if opts.SkipInvalidRows {
				// Try to merge extra fields into the last column
				merged := make([]string, expectedCols)
				copy(merged, record[:expectedCols-1])
				merged[expectedCols-1] = strings.Join(record[expectedCols-1:], " ")
				normalized = append(normalized, merged)
			}
		} else {
			normalized = append(normalized, record)
		}
	}
	
	return normalized
}

// fuzzyInferColumnTypes infers types with more lenient rules
func fuzzyInferColumnTypes(sampleData [][]string, numCols int, opts *FuzzyImportOptions) []storage.ColType {
	types := make([]storage.ColType, numCols)
	
	nullLiterals := []string{}
	if opts.ImportOptions != nil {
		nullLiterals = opts.ImportOptions.NullLiterals
	}
	
	for colIdx := 0; colIdx < numCols; colIdx++ {
		typeVotes := make(map[storage.ColType]int)
		totalValues := 0
		
		for _, row := range sampleData {
			if colIdx >= len(row) {
				continue
			}
			
			value := strings.TrimSpace(row[colIdx])
			if value == "" || isNullValue(value, nullLiterals) {
				continue
			}
			
			totalValues++
			detectedType := fuzzyDetectType(value, opts)
			typeVotes[detectedType]++
		}
		
		// Determine the dominant type
		if totalValues == 0 {
			types[colIdx] = storage.TextType
			continue
		}
		
		// Find type with most votes
		maxVotes := 0
		var dominantType storage.ColType
		for typ, votes := range typeVotes {
			if votes > maxVotes {
				maxVotes = votes
				dominantType = typ
			}
		}
		
		// If type consistency is low and mixed types are allowed, use TEXT
		consistency := float64(maxVotes) / float64(totalValues)
		if consistency < 0.8 && opts.AllowMixedTypes {
			types[colIdx] = storage.TextType
		} else {
			types[colIdx] = dominantType
		}
	}
	
	return types
}

// fuzzyDetectType detects type with lenient parsing
func fuzzyDetectType(value string, opts *FuzzyImportOptions) storage.ColType {
	value = strings.TrimSpace(value)
	
	// Remove common thousand separators
	cleanValue := strings.ReplaceAll(value, ",", "")
	cleanValue = strings.ReplaceAll(cleanValue, " ", "")
	
	// Try boolean
	lower := strings.ToLower(value)
	if lower == "true" || lower == "false" || lower == "yes" || lower == "no" ||
		lower == "t" || lower == "f" || lower == "y" || lower == "n" ||
		value == "1" || value == "0" {
		return storage.BoolType
	}
	
	// Try integer
	if _, err := strconv.ParseInt(cleanValue, 10, 64); err == nil {
		return storage.IntType
	}
	
	// Try float
	if _, err := strconv.ParseFloat(cleanValue, 64); err == nil {
		return storage.Float64Type
	}
	
	// Try to detect JSON
	if opts.FuzzyJSON && (strings.HasPrefix(value, "{") || strings.HasPrefix(value, "[")) {
		var js interface{}
		if err := json.Unmarshal([]byte(value), &js); err == nil {
			return storage.JsonType
		}
	}
	
	// Default to text
	return storage.TextType
}

// fuzzyConvertValue converts a string value to the target type with lenient parsing
func fuzzyConvertValue(value string, targetType storage.ColType, opts *FuzzyImportOptions) (interface{}, error) {
	value = strings.TrimSpace(value)
	
	nullLiterals := []string{}
	if opts.ImportOptions != nil {
		nullLiterals = opts.ImportOptions.NullLiterals
	}
	
	// Check for null
	if value == "" || isNullValue(value, nullLiterals) {
		return nil, nil
	}
	
	switch targetType {
	case storage.IntType:
		// Remove common formatting
		cleanValue := removeNonNumeric(value, false)
		if cleanValue == "" {
			return nil, fmt.Errorf("invalid integer: %s", value)
		}
		i, err := strconv.ParseInt(cleanValue, 10, 64)
		if err != nil {
			// Try parsing as float and converting
			if f, err2 := strconv.ParseFloat(cleanValue, 64); err2 == nil {
				return int(f), nil
			}
			return nil, fmt.Errorf("invalid integer: %s", value)
		}
		// Return int instead of int64 for compatibility with SQL literals
		return int(i), nil
		
	case storage.Float64Type, storage.FloatType:
		cleanValue := removeNonNumeric(value, true)
		if cleanValue == "" {
			return nil, fmt.Errorf("invalid float: %s", value)
		}
		f, err := strconv.ParseFloat(cleanValue, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid float: %s", value)
		}
		return f, nil
		
	case storage.BoolType:
		lower := strings.ToLower(value)
		switch lower {
		case "true", "yes", "y", "t", "1", "on":
			return true, nil
		case "false", "no", "n", "f", "0", "off":
			return false, nil
		default:
			return nil, fmt.Errorf("invalid boolean: %s", value)
		}
		
	case storage.JsonType:
		var result interface{}
		if err := json.Unmarshal([]byte(value), &result); err != nil {
			if opts.FuzzyJSON {
				// Try to fix common JSON issues
				fixed := fixCommonJSONIssues(value)
				if err2 := json.Unmarshal([]byte(fixed), &result); err2 == nil {
					return result, nil
				}
			}
			return nil, fmt.Errorf("invalid JSON: %s", value)
		}
		return result, nil
		
	default:
		return value, nil
	}
}

// removeNonNumeric removes non-numeric characters but keeps signs and decimals
func removeNonNumeric(s string, allowDecimal bool) string {
	var result strings.Builder
	hasDecimal := false
	hasSign := false
	
	for _, r := range s {
		if unicode.IsDigit(r) {
			result.WriteRune(r)
		} else if !hasSign && (r == '-' || r == '+') && result.Len() == 0 {
			result.WriteRune(r)
			hasSign = true
		} else if allowDecimal && !hasDecimal && (r == '.' || r == ',') {
			result.WriteRune('.')
			hasDecimal = true
		}
	}
	
	return result.String()
}

// fixCommonJSONIssues attempts to fix common JSON formatting issues
func fixCommonJSONIssues(s string) string {
	// Replace single quotes with double quotes
	s = strings.ReplaceAll(s, "'", "\"")
	
	// Fix unquoted keys (simple cases)
	re := regexp.MustCompile(`([{,]\s*)([a-zA-Z_][a-zA-Z0-9_]*)\s*:`)
	s = re.ReplaceAllString(s, `$1"$2":`)
	
	return s
}

// FuzzyImportJSON attempts to parse malformed JSON data
func FuzzyImportJSON(
	ctx context.Context,
	db *storage.DB,
	tenant string,
	tableName string,
	src io.Reader,
	opts *FuzzyImportOptions,
) (*ImportResult, error) {
	if opts == nil {
		opts = &FuzzyImportOptions{
			ImportOptions: &ImportOptions{},
		}
	}
	applyFuzzyDefaults(opts)
	
	data, err := io.ReadAll(src)
	if err != nil {
		return nil, err
	}
	
	dataStr := string(data)
	
	// Try to fix common JSON issues
	if opts.FuzzyJSON {
		dataStr = fixCommonJSONIssues(dataStr)
	}
	
	// Try parsing as array
	var records []map[string]interface{}
	err = json.Unmarshal([]byte(dataStr), &records)
	
	if err != nil {
		// Try as single object
		var record map[string]interface{}
		if err2 := json.Unmarshal([]byte(dataStr), &record); err2 == nil {
			records = []map[string]interface{}{record}
		} else {
			// Try line-delimited JSON
			records, err = parseLineDelimitedJSON(dataStr, opts)
			if err != nil {
				return nil, fmt.Errorf("failed to parse JSON: %v", err)
			}
		}
	}
	
	if len(records) == 0 {
		return &ImportResult{}, nil
	}
	
	// Extract all unique keys
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
	
	// Create columns (all as JSON type initially)
	columns := make([]storage.Column, len(columnNames))
	for i, name := range columnNames {
		columns[i] = storage.Column{
			Name: name,
			Type: storage.JsonType,
		}
	}
	
	result := &ImportResult{
		ColumnNames: columnNames,
		ColumnTypes: make([]storage.ColType, len(columnNames)),
		Errors:      make([]string, 0),
	}
	
	for i := range result.ColumnTypes {
		result.ColumnTypes[i] = storage.JsonType
	}
	
	// Create table
	if opts.CreateTable {
		table := storage.NewTable(tableName, columns, false)
		if err := db.Put(tenant, table); err != nil {
			return nil, err
		}
	}
	
	table, err := db.Get(tenant, tableName)
	if err != nil {
		return nil, err
	}
	
	// Insert records
	for _, record := range records {
		row := make([]interface{}, len(columnNames))
		for i, colName := range columnNames {
			if value, exists := record[colName]; exists {
				row[i] = value
			}
		}
		table.Rows = append(table.Rows, row)
		result.RowsInserted++
	}
	
	return result, nil
}

// parseLineDelimitedJSON parses newline-delimited JSON (NDJSON)
func parseLineDelimitedJSON(data string, opts *FuzzyImportOptions) ([]map[string]interface{}, error) {
	var records []map[string]interface{}
	
	scanner := bufio.NewScanner(strings.NewReader(data))
	lineNum := 0
	
	for scanner.Scan() {
		lineNum++
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		
		var record map[string]interface{}
		err := json.Unmarshal([]byte(line), &record)
		if err != nil {
			if opts.FuzzyJSON {
				fixed := fixCommonJSONIssues(line)
				if err2 := json.Unmarshal([]byte(fixed), &record); err2 == nil {
					records = append(records, record)
					continue
				}
			}
			// Skip invalid lines in fuzzy mode
			if opts.SkipInvalidRows {
				continue
			}
			return nil, fmt.Errorf("invalid JSON on line %d: %v", lineNum, err)
		}
		records = append(records, record)
	}
	
	return records, scanner.Err()
}
