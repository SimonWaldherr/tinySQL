// Excel (.xlsx) import. XLSX is a ZIP archive of XML parts (the Office Open
// XML / ECMA-376 SpreadsheetML format): a workbook listing sheets, a
// worksheet XML per sheet holding cell values, an optional shared-string
// table (repeated text is stored once and referenced by index), and an
// optional styles part this importer consults only to recognize date/time
// number formats. No third-party XLSX library is used — archive/zip and
// encoding/xml (both stdlib) are enough for the read path, consistent with
// this package's existing hand-rolled KML/OSM/Shapefile readers.
package importer

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// xlsxEpoch is day 0 of Excel's "1900 date system" serial numbers, chosen as
// December 30 1899 (not January 1 1900) so serial-to-time conversion is
// correct across the well-known Lotus 1-2-3 leap-year bug Excel deliberately
// preserves (it treats 1900 as a leap year), the standard fix every
// spreadsheet library applies.
var xlsxEpoch = time.Date(1899, time.December, 30, 0, 0, 0, 0, time.UTC)

// xlsxBuiltinDateNumFmtIDs are the ECMA-376 built-in number-format IDs that
// represent a date and/or time. Custom formats (id >= 164) are instead
// classified by inspecting their format-code text; see isXLSXDateFormatCode.
var xlsxBuiltinDateNumFmtIDs = map[int]bool{
	14: true, 15: true, 16: true, 17: true, 18: true, 19: true, 20: true,
	21: true, 22: true, 45: true, 46: true, 47: true,
}

type xlsxWorkbookXML struct {
	Sheets struct {
		Sheet []struct {
			Name string `xml:"name,attr"`
			RID  string `xml:"http://schemas.openxmlformats.org/officeDocument/2006/relationships id,attr"`
		} `xml:"sheet"`
	} `xml:"sheets"`
}

type xlsxRelationshipsXML struct {
	Relationship []struct {
		ID     string `xml:"Id,attr"`
		Target string `xml:"Target,attr"`
	} `xml:"Relationship"`
}

type xlsxSSTXML struct {
	SI []struct {
		T string `xml:"t"`
		R []struct {
			T string `xml:"t"`
		} `xml:"r"`
	} `xml:"si"`
}

type xlsxStylesXML struct {
	NumFmts struct {
		NumFmt []struct {
			ID   int    `xml:"numFmtId,attr"`
			Code string `xml:"formatCode,attr"`
		} `xml:"numFmt"`
	} `xml:"numFmts"`
	CellXfs struct {
		Xf []struct {
			NumFmtID int `xml:"numFmtId,attr"`
		} `xml:"xf"`
	} `xml:"cellXfs"`
}

// xlsxCell is one <c> spreadsheet cell element: r names its column+row
// (e.g. "C7"), t its value type (shared string/inline string/boolean/error;
// absent means numeric), s its style index (consulted only for date/time
// number-format detection), and v (or is for inline strings) its raw value.
type xlsxCell struct {
	R  string `xml:"r,attr"`
	T  string `xml:"t,attr"`
	S  string `xml:"s,attr"`
	V  string `xml:"v"`
	Is struct {
		T string `xml:"t"`
	} `xml:"is"`
}

type xlsxWorksheetXML struct {
	SheetData struct {
		Row []struct {
			C []xlsxCell `xml:"c"`
		} `xml:"row"`
	} `xml:"sheetData"`
}

// ImportXLSX imports the first worksheet (or opts.XLSXSheet, if set) of an
// Excel workbook from src into a tinySQL table.
//
// Cell values are read with their native Excel type: numbers stay numeric,
// booleans stay boolean, and a numeric cell styled with a recognized
// date/time number format is converted from its Excel serial date to a
// timestamp — all fed through the same sample-based type inference and
// insertion path ImportCSV uses, so column typing behaves identically
// whether the source was a spreadsheet or a delimited file.
func ImportXLSX(
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

	limited := limitInput(ctx, src, opts)
	data, err := io.ReadAll(limited)
	if err != nil {
		return nil, fmt.Errorf("read xlsx: %w", err)
	}
	zr, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return nil, fmt.Errorf("open xlsx: %w", err)
	}

	sheetPath, err := resolveXLSXSheetPath(zr, opts.XLSXSheet)
	if err != nil {
		return nil, err
	}
	sst, err := loadXLSXSharedStrings(zr)
	if err != nil {
		return nil, err
	}
	dateStyles, err := loadXLSXDateStyles(zr)
	if err != nil {
		return nil, err
	}
	grid, err := readXLSXSheet(zr, sheetPath, sst, dateStyles)
	if err != nil {
		return nil, err
	}
	if len(grid) == 0 {
		return nil, fmt.Errorf("no rows found in xlsx sheet")
	}

	result := &ImportResult{Encoding: "utf-8", Errors: make([]string, 0)}

	hasHeader := decideHeader(grid, opts.HeaderMode)
	result.HadHeader = hasHeader
	var colNames []string
	var dataRows [][]string
	if hasHeader {
		colNames = sanitizeColumnNames(grid[0])
		dataRows = grid[1:]
	} else {
		colNames = generateColumnNames(len(grid[0]))
		dataRows = grid
	}
	result.ColumnNames = colNames

	sample := dataRows
	if opts.SampleRecords > 0 && len(sample) > opts.SampleRecords {
		sample = sample[:opts.SampleRecords]
	}
	var colTypes []storage.ColType
	if opts.TypeInference {
		colTypes = inferColumnTypes(sample, len(colNames), opts)
	} else {
		colTypes = make([]storage.ColType, len(colNames))
		for i := range colTypes {
			colTypes[i] = storage.TextType
		}
	}
	result.ColumnTypes = colTypes

	if opts.CreateTable {
		if err := createTable(ctx, db, tenant, tableName, colNames, colTypes); err != nil {
			return nil, fmt.Errorf("create table: %w", err)
		}
	}
	if opts.Truncate {
		if err := truncateTable(ctx, db, tenant, tableName); err != nil {
			return nil, fmt.Errorf("truncate table: %w", err)
		}
	}

	rows, skipped, errs := insertAllRecords(ctx, db, tenant, tableName, colNames, colTypes, dataRows, opts)
	result.RowsInserted = rows
	result.RowsSkipped = skipped
	result.Errors = append(result.Errors, errs...)
	return result, nil
}

// resolveXLSXSheetPath finds the in-zip path of the requested worksheet
// (opts.XLSXSheet by name, or the first declared sheet if empty) by
// following workbook.xml's sheet list through its relationship file — a
// sheet's r:id, not its position, is what names its actual sheetN.xml part.
func resolveXLSXSheetPath(zr *zip.Reader, sheetName string) (string, error) {
	wb, err := readXLSXXML[xlsxWorkbookXML](zr, "xl/workbook.xml")
	if err != nil {
		return "", fmt.Errorf("read workbook.xml: %w", err)
	}
	if len(wb.Sheets.Sheet) == 0 {
		return "", fmt.Errorf("workbook has no sheets")
	}
	rels, err := readXLSXXML[xlsxRelationshipsXML](zr, "xl/_rels/workbook.xml.rels")
	if err != nil {
		return "", fmt.Errorf("read workbook.xml.rels: %w", err)
	}
	targets := make(map[string]string, len(rels.Relationship))
	for _, r := range rels.Relationship {
		targets[r.ID] = r.Target
	}

	sel := wb.Sheets.Sheet[0]
	if sheetName != "" {
		found := false
		for _, s := range wb.Sheets.Sheet {
			if s.Name == sheetName {
				sel = s
				found = true
				break
			}
		}
		if !found {
			return "", fmt.Errorf("sheet %q not found", sheetName)
		}
	}
	target, ok := targets[sel.RID]
	if !ok {
		return "", fmt.Errorf("no relationship target for sheet %q", sel.Name)
	}
	target = strings.TrimPrefix(target, "/xl/")
	target = strings.TrimPrefix(target, "xl/")
	return "xl/" + target, nil
}

// loadXLSXSharedStrings reads xl/sharedStrings.xml, if present — a workbook
// with no text cells at all may omit it entirely, which is not an error.
func loadXLSXSharedStrings(zr *zip.Reader) ([]string, error) {
	f := findXLSXFile(zr, "xl/sharedStrings.xml")
	if f == nil {
		return nil, nil
	}
	sst, err := readXLSXXMLFile[xlsxSSTXML](f)
	if err != nil {
		return nil, fmt.Errorf("read sharedStrings.xml: %w", err)
	}
	out := make([]string, len(sst.SI))
	for i, si := range sst.SI {
		if si.T != "" || len(si.R) == 0 {
			out[i] = si.T
			continue
		}
		var b strings.Builder
		for _, r := range si.R {
			b.WriteString(r.T)
		}
		out[i] = b.String()
	}
	return out, nil
}

// loadXLSXDateStyles reads xl/styles.xml, if present, and returns the set of
// cell-style indexes (a cell's "s" attribute) whose number format is a
// recognized date/time format.
func loadXLSXDateStyles(zr *zip.Reader) (map[int]bool, error) {
	f := findXLSXFile(zr, "xl/styles.xml")
	if f == nil {
		return nil, nil
	}
	styles, err := readXLSXXMLFile[xlsxStylesXML](f)
	if err != nil {
		return nil, fmt.Errorf("read styles.xml: %w", err)
	}
	customDateFmt := make(map[int]bool, len(styles.NumFmts.NumFmt))
	for _, nf := range styles.NumFmts.NumFmt {
		if isXLSXDateFormatCode(nf.Code) {
			customDateFmt[nf.ID] = true
		}
	}
	dateStyles := make(map[int]bool)
	for i, xf := range styles.CellXfs.Xf {
		if xlsxBuiltinDateNumFmtIDs[xf.NumFmtID] || customDateFmt[xf.NumFmtID] {
			dateStyles[i] = true
		}
	}
	return dateStyles, nil
}

// isXLSXDateFormatCode heuristically classifies a custom number-format code
// (e.g. "yyyy-mm-dd", "h:mm:ss") as a date/time format by looking for its
// characteristic letters outside of quoted literal text — the same
// approach most minimal XLSX readers use, since fully parsing the format
// mini-language is unnecessary just to distinguish "this is a date" from
// "this is a plain number".
func isXLSXDateFormatCode(code string) bool {
	inLiteral := false
	for _, r := range code {
		if r == '"' {
			inLiteral = !inLiteral
			continue
		}
		if inLiteral {
			continue
		}
		switch r {
		case 'y', 'Y', 'm', 'M', 'd', 'D', 'h', 'H', 's', 'S':
			return true
		}
	}
	return false
}

// readXLSXSheet decodes one worksheet part into a rectangular [][]string
// grid. Cells are stored sparsely in XLSX (only non-empty cells appear, each
// naming its own column via its "r" reference like "C7"), so this collects
// every row as a column-index-keyed sparse map first and only pads to a
// common width once the widest row is known — a header that happens to be
// narrower than some data row must not misalign the columns after it.
func readXLSXSheet(zr *zip.Reader, sheetPath string, sst []string, dateStyles map[int]bool) ([][]string, error) {
	f := findXLSXFile(zr, sheetPath)
	if f == nil {
		return nil, fmt.Errorf("worksheet part %q not found in workbook", sheetPath)
	}
	ws, err := readXLSXXMLFile[xlsxWorksheetXML](f)
	if err != nil {
		return nil, fmt.Errorf("read worksheet %q: %w", sheetPath, err)
	}

	sparseRows := make([]map[int]string, len(ws.SheetData.Row))
	width := 0
	for ri, row := range ws.SheetData.Row {
		cells := make(map[int]string, len(row.C))
		nextCol := 0
		for _, c := range row.C {
			col := nextCol
			if c.R != "" {
				if parsed, ok := xlsxColumnIndex(c.R); ok {
					col = parsed
				}
			}
			nextCol = col + 1
			if col+1 > width {
				width = col + 1
			}
			cells[col] = xlsxCellString(c, sst, dateStyles)
		}
		sparseRows[ri] = cells
	}

	grid := make([][]string, len(sparseRows))
	for ri, cells := range sparseRows {
		row := make([]string, width)
		for col, val := range cells {
			row[col] = val
		}
		grid[ri] = row
	}
	return grid, nil
}

// xlsxCellString resolves one <c> element's display text per its declared
// type: a shared-string index, an inline string, a boolean, or (the default
// when t is absent) a plain number — converted to an RFC3339 timestamp
// first if its style marks it as a date/time cell.
func xlsxCellString(c xlsxCell, sst []string, dateStyles map[int]bool) string {
	switch c.T {
	case "s":
		idx, err := strconv.Atoi(strings.TrimSpace(c.V))
		if err != nil || idx < 0 || idx >= len(sst) {
			return ""
		}
		return sst[idx]
	case "inlineStr":
		return c.Is.T
	case "b":
		if strings.TrimSpace(c.V) == "1" {
			return "true"
		}
		return "false"
	case "str", "e":
		return c.V
	default:
		if c.V == "" {
			return ""
		}
		if c.S != "" {
			if s, err := strconv.Atoi(c.S); err == nil && dateStyles[s] {
				if serial, err := strconv.ParseFloat(c.V, 64); err == nil {
					t := xlsxEpoch.Add(time.Duration(serial * 24 * float64(time.Hour)))
					return t.Format(time.RFC3339)
				}
			}
		}
		return c.V
	}
}

// xlsxColumnIndex extracts the 0-based column index from a cell reference
// like "A1" or "AB37" (the base-26 letter prefix; Excel's column naming has
// no digit for zero, so "A"=0, "Z"=25, "AA"=26, matching a bijective
// base-26 numeral system).
func xlsxColumnIndex(ref string) (int, bool) {
	i := 0
	for i < len(ref) && ref[i] >= 'A' && ref[i] <= 'Z' {
		i++
	}
	if i == 0 {
		return 0, false
	}
	col := 0
	for _, r := range ref[:i] {
		col = col*26 + int(r-'A'+1)
	}
	return col - 1, true
}

func findXLSXFile(zr *zip.Reader, name string) *zip.File {
	for _, f := range zr.File {
		if f.Name == name {
			return f
		}
	}
	return nil
}

func readXLSXXML[T any](zr *zip.Reader, name string) (*T, error) {
	f := findXLSXFile(zr, name)
	if f == nil {
		return nil, fmt.Errorf("part %q not found in workbook", name)
	}
	return readXLSXXMLFile[T](f)
}

func readXLSXXMLFile[T any](f *zip.File) (*T, error) {
	rc, err := f.Open()
	if err != nil {
		return nil, err
	}
	defer func() { _ = rc.Close() }()
	var v T
	if err := xml.NewDecoder(rc).Decode(&v); err != nil {
		return nil, err
	}
	return &v, nil
}
