// Excel (.xlsx) export. Writes a minimal but valid Office Open XML
// SpreadsheetML workbook: one sheet, no shared-string table (every text
// value is written as an inline string, which the format supports natively
// and avoids the bookkeeping of a separate dictionary part), and the
// boilerplate parts (content types, relationships, a single default style)
// every reader — including Excel itself — expects to find. Built with only
// archive/zip and encoding/xml (both stdlib), matching this package's
// existing hand-rolled GeoJSON/TopoJSON/SQL writers: no third-party XLSX
// dependency.
package exporter

import (
	"archive/zip"
	"encoding/xml"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/SimonWaldherr/tinySQL/internal/engine"
)

const xlsxContentTypesXML = `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">
<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>
<Default Extension="xml" ContentType="application/xml"/>
<Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>
<Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>
<Override PartName="/xl/styles.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml"/>
</Types>
`

const xlsxRootRelsXML = `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/>
</Relationships>
`

const xlsxWorkbookRelsXML = `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/>
<Relationship Id="rId2" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles" Target="styles.xml"/>
</Relationships>
`

const xlsxWorkbookXMLTemplate = `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">
<sheets><sheet name="%s" sheetId="1" r:id="rId1"/></sheets>
</workbook>
`

// xlsxStylesXML defines exactly one cell format (the default), which is all
// a reader-only-of-values export needs; Excel requires this part to exist
// at all, even trivially, or it opens the file with a repair prompt.
const xlsxStylesXML = `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<styleSheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">
<fonts count="1"><font><sz val="11"/><name val="Calibri"/></font></fonts>
<fills count="1"><fill><patternFill patternType="none"/></fill></fills>
<borders count="1"><border><left/><right/><top/><bottom/><diagonal/></border></borders>
<cellStyleXfs count="1"><xf numFmtId="0" fontId="0" fillId="0" borderId="0"/></cellStyleXfs>
<cellXfs count="1"><xf numFmtId="0" fontId="0" fillId="0" borderId="0" xfId="0"/></cellXfs>
</styleSheet>
`

// ExportXLSX writes rs as a single-sheet Excel workbook to w. Numeric Go
// values (int*/uint*/float*) become native numeric cells and bool becomes a
// native boolean cell, so a SUM/AVERAGE formula over an exported numeric
// column works without a manual "convert to number" step in Excel; every
// other value (including time.Time, formatted like the other exporters'
// valueToString) is written as an inline string cell.
func ExportXLSX(w io.Writer, rs *engine.ResultSet, opts Options) error {
	zw := zip.NewWriter(w)

	if err := xlsxWriteRaw(zw, "[Content_Types].xml", xlsxContentTypesXML); err != nil {
		return err
	}
	if err := xlsxWriteRaw(zw, "_rels/.rels", xlsxRootRelsXML); err != nil {
		return err
	}
	if err := xlsxWriteRaw(zw, "xl/_rels/workbook.xml.rels", xlsxWorkbookRelsXML); err != nil {
		return err
	}
	if err := xlsxWriteRaw(zw, "xl/styles.xml", xlsxStylesXML); err != nil {
		return err
	}
	if err := xlsxWriteRaw(zw, "xl/workbook.xml", fmt.Sprintf(xlsxWorkbookXMLTemplate, "Sheet1")); err != nil {
		return err
	}

	sheet, err := zw.Create("xl/worksheets/sheet1.xml")
	if err != nil {
		return err
	}
	if err := xlsxWriteSheet(sheet, rs, opts); err != nil {
		return err
	}

	return zw.Close()
}

func xlsxWriteRaw(zw *zip.Writer, name, content string) error {
	f, err := zw.Create(name)
	if err != nil {
		return err
	}
	_, err = io.WriteString(f, content)
	return err
}

func xlsxWriteSheet(w io.Writer, rs *engine.ResultSet, opts Options) error {
	if _, err := io.WriteString(w, `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>`+"\n"+
		`<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><sheetData>`+"\n"); err != nil {
		return err
	}

	if _, err := io.WriteString(w, xlsxHeaderRow(1, rs.Cols)); err != nil {
		return err
	}

	for i, r := range rs.Rows {
		rowNum := i + 2 // row 1 is the header
		var b strings.Builder
		fmt.Fprintf(&b, `<row r="%d">`, rowNum)
		for ci, c := range rs.Cols {
			ref := xlsxCellRef(ci, rowNum)
			b.WriteString(xlsxWriteCell(ref, r[strings.ToLower(c)], opts.BinaryEncoding))
		}
		b.WriteString("</row>\n")
		if _, err := io.WriteString(w, b.String()); err != nil {
			return err
		}
	}

	_, err := io.WriteString(w, "</sheetData></worksheet>\n")
	return err
}

func xlsxHeaderRow(rowNum int, cols []string) string {
	var b strings.Builder
	fmt.Fprintf(&b, `<row r="%d">`, rowNum)
	for i, c := range cols {
		b.WriteString(xlsxInlineStringCell(xlsxCellRef(i, rowNum), c))
	}
	b.WriteString("</row>\n")
	return b.String()
}

// xlsxWriteCell renders one data cell, choosing a native numeric/boolean
// XLSX cell type when v's Go type allows it and an inline string otherwise.
func xlsxWriteCell(ref string, v any, binaryEncoding string) string {
	if v == nil {
		return ""
	}
	switch t := v.(type) {
	case bool:
		val := "0"
		if t {
			val = "1"
		}
		return fmt.Sprintf(`<c r="%s" t="b"><v>%s</v></c>`, ref, val)
	case int:
		return xlsxNumericCell(ref, strconv.FormatInt(int64(t), 10))
	case int8:
		return xlsxNumericCell(ref, strconv.FormatInt(int64(t), 10))
	case int16:
		return xlsxNumericCell(ref, strconv.FormatInt(int64(t), 10))
	case int32:
		return xlsxNumericCell(ref, strconv.FormatInt(int64(t), 10))
	case int64:
		return xlsxNumericCell(ref, strconv.FormatInt(t, 10))
	case uint:
		return xlsxNumericCell(ref, strconv.FormatUint(uint64(t), 10))
	case uint8:
		return xlsxNumericCell(ref, strconv.FormatUint(uint64(t), 10))
	case uint16:
		return xlsxNumericCell(ref, strconv.FormatUint(uint64(t), 10))
	case uint32:
		return xlsxNumericCell(ref, strconv.FormatUint(uint64(t), 10))
	case uint64:
		return xlsxNumericCell(ref, strconv.FormatUint(t, 10))
	case float32:
		return xlsxNumericCell(ref, strconv.FormatFloat(float64(t), 'g', -1, 32))
	case float64:
		return xlsxNumericCell(ref, strconv.FormatFloat(t, 'g', -1, 64))
	case time.Time:
		return xlsxInlineStringCell(ref, t.Format(time.RFC3339))
	default:
		return xlsxInlineStringCell(ref, valueToString(v, binaryEncoding))
	}
}

func xlsxNumericCell(ref, num string) string {
	return fmt.Sprintf(`<c r="%s"><v>%s</v></c>`, ref, num)
}

func xlsxInlineStringCell(ref, text string) string {
	var buf strings.Builder
	if err := xml.EscapeText(&buf, []byte(text)); err != nil {
		buf.Reset()
		buf.WriteString(text)
	}
	return fmt.Sprintf(`<c r="%s" t="inlineStr"><is><t xml:space="preserve">%s</t></is></c>`, ref, buf.String())
}

// xlsxCellRef renders a 0-based (col, row) pair as an Excel cell reference
// like "A1"/"AB37" — the inverse of the importer's xlsxColumnIndex.
func xlsxCellRef(col, row int) string {
	var letters string
	n := col + 1
	for n > 0 {
		n--
		letters = string(rune('A'+n%26)) + letters
		n /= 26
	}
	return fmt.Sprintf("%s%d", letters, row)
}
