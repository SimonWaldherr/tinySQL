package importer

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// buildMinimalXLSX hand-constructs a one-sheet workbook with a single
// "event_date" column: an inline-string header, plus one data row holding a
// numeric cell styled (via its "s" attribute + styles.xml's custom date
// number format) as a date, so ImportXLSX's serial-date conversion path can
// be exercised without depending on a real Excel-produced fixture file.
func buildMinimalXLSX(t *testing.T, dateCellValue string) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	write := func(name, content string) {
		f, err := zw.Create(name)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := f.Write([]byte(content)); err != nil {
			t.Fatal(err)
		}
	}
	write("xl/workbook.xml", `<?xml version="1.0"?>`+
		`<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">`+
		`<sheets><sheet name="Sheet1" sheetId="1" r:id="rId1"/></sheets></workbook>`)
	write("xl/_rels/workbook.xml.rels", `<?xml version="1.0"?>`+
		`<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">`+
		`<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/>`+
		`</Relationships>`)
	write("xl/styles.xml", `<?xml version="1.0"?>`+
		`<styleSheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">`+
		`<numFmts count="1"><numFmt numFmtId="164" formatCode="yyyy-mm-dd"/></numFmts>`+
		`<cellXfs count="2"><xf numFmtId="0"/><xf numFmtId="164"/></cellXfs>`+
		`</styleSheet>`)
	sheetXML := fmt.Sprintf(`<?xml version="1.0"?>`+
		`<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><sheetData>`+
		`<row r="1"><c r="A1" t="inlineStr"><is><t>event_date</t></is></c></row>`+
		`<row r="2"><c r="A2" s="1"><v>%s</v></c></row>`+
		`</sheetData></worksheet>`, dateCellValue)
	write("xl/worksheets/sheet1.xml", sheetXML)
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func TestImportXLSXDateConversion(t *testing.T) {
	ref := time.Date(2021, 3, 15, 0, 0, 0, 0, time.UTC)
	serialDays := ref.Sub(xlsxEpoch).Hours() / 24
	data := buildMinimalXLSX(t, fmt.Sprintf("%.0f", serialDays))

	db := storage.NewDB()
	ctx := context.Background()
	// HeaderMode is forced to "present": decideHeader's numeric-vs-text
	// heuristic (shared with CSV import) expects a data column to look
	// numeric to tell it apart from a text header, but a date cell is
	// already converted to an RFC3339 string by the time decideHeader sees
	// it (see xlsxCellString) — with only one data row, that string doesn't
	// "look numeric", and detection would misfire. Real workbooks with more
	// rows/columns give the heuristic plenty of other signal; this minimal
	// single-row fixture does not, so this test is explicit instead.
	opts := &ImportOptions{CreateTable: true, TypeInference: true, HeaderMode: "present"}
	result, err := ImportXLSX(ctx, db, "default", "events", bytes.NewReader(data), opts)
	if err != nil {
		t.Fatalf("ImportXLSX: %v", err)
	}
	if result.RowsInserted != 1 {
		t.Fatalf("RowsInserted = %d, want 1", result.RowsInserted)
	}

	tbl, err := db.Get("default", "events")
	if err != nil {
		t.Fatal(err)
	}
	got, ok := tbl.Rows[0][0].(time.Time)
	if !ok {
		t.Fatalf("event_date = %v (%T), want time.Time", tbl.Rows[0][0], tbl.Rows[0][0])
	}
	if got.Year() != 2021 || got.Month() != time.March || got.Day() != 15 {
		t.Fatalf("event_date = %v, want 2021-03-15", got)
	}
}

func TestImportXLSXStrictTypesKeepsValidRowsAroundBadRow(t *testing.T) {
	base := buildMinimalXLSX(t, "1")
	zr, err := zip.NewReader(bytes.NewReader(base), int64(len(base)))
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	for _, part := range zr.File {
		out, err := zw.Create(part.Name)
		if err != nil {
			t.Fatal(err)
		}
		if part.Name == "xl/worksheets/sheet1.xml" {
			_, err = io.WriteString(out, `<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><sheetData>`+
				`<row><c r="A1" t="inlineStr"><is><t>num</t></is></c></row>`+
				`<row><c r="A2"><v>12</v></c></row>`+
				`<row><c r="A3" t="inlineStr"><is><t>bad</t></is></c></row>`+
				`<row><c r="A4"><v>34</v></c></row></sheetData></worksheet>`)
		} else {
			var in io.ReadCloser
			in, err = part.Open()
			if err == nil {
				_, err = io.Copy(out, in)
				closeErr := in.Close()
				if err == nil {
					err = closeErr
				}
			}
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	db := storage.NewDB()
	result, err := ImportXLSX(context.Background(), db, "default", "numbers", bytes.NewReader(buf.Bytes()),
		&ImportOptions{CreateTable: true, HeaderMode: "present", TypeInference: true, StrictTypes: true, SampleRecords: 1})
	if err != nil {
		t.Fatal(err)
	}
	if result.RowsInserted != 2 || result.RowsSkipped != 1 || len(result.Errors) != 1 {
		t.Fatalf("result = %+v", result)
	}
	tbl, err := db.Get("default", "numbers")
	if err != nil {
		t.Fatal(err)
	}
	if len(tbl.Rows) != 2 || tbl.Rows[0][0] != int64(12) || tbl.Rows[1][0] != int64(34) {
		t.Fatalf("rows = %#v", tbl.Rows)
	}
}

func TestXLSXColumnIndex(t *testing.T) {
	cases := []struct {
		ref  string
		want int
		ok   bool
	}{
		{"A1", 0, true},
		{"B7", 1, true},
		{"Z1", 25, true},
		{"AA1", 26, true},
		{"AB37", 27, true},
		{"", 0, false},
		{"1", 0, false},
	}
	for _, c := range cases {
		got, ok := xlsxColumnIndex(c.ref)
		if ok != c.ok || (ok && got != c.want) {
			t.Errorf("xlsxColumnIndex(%q) = (%d, %v), want (%d, %v)", c.ref, got, ok, c.want, c.ok)
		}
	}
}

func TestXLSXInlineRichText(t *testing.T) {
	var cell xlsxCell
	if err := xml.Unmarshal([]byte(`<c r="A1" t="inlineStr"><is><r><t xml:space="preserve">hello </t></r><r><t>world</t></r></is></c>`), &cell); err != nil {
		t.Fatal(err)
	}
	if got := xlsxCellString(cell, nil, nil); got != "hello world" {
		t.Fatalf("rich inline text = %q", got)
	}
}

func TestIsXLSXDateFormatCode(t *testing.T) {
	cases := []struct {
		code string
		want bool
	}{
		{"yyyy-mm-dd", true},
		{"h:mm:ss", true},
		{"0.00", false},
		{"#,##0", false},
		{`"Total: "0.00`, false}, // date-like letters only appear inside a quoted literal
	}
	for _, c := range cases {
		if got := isXLSXDateFormatCode(c.code); got != c.want {
			t.Errorf("isXLSXDateFormatCode(%q) = %v, want %v", c.code, got, c.want)
		}
	}
}
