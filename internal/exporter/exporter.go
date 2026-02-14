package exporter

import (
    "encoding/csv"
    "encoding/gob"
    "encoding/json"
    "encoding/xml"
    "fmt"
    "io"
    "strconv"
    "strings"
    "time"

    "github.com/SimonWaldherr/tinySQL/internal/engine"
)

func init() {
    // Register common concrete types stored in engine.Row (used as interface{}).
    gob.Register(time.Time{})
}

// Options controls exporter behavior.
type Options struct {
    PrettyJSON  bool
    CSVNoHeader bool
    CSVDelimiter rune
}

func valueToString(v any) string {
    if v == nil {
        return ""
    }
    switch t := v.(type) {
    case string:
        return t
    case bool:
        return strconv.FormatBool(t)
    case int:
        return strconv.FormatInt(int64(t), 10)
    case int8:
        return strconv.FormatInt(int64(t), 10)
    case int16:
        return strconv.FormatInt(int64(t), 10)
    case int32:
        return strconv.FormatInt(int64(t), 10)
    case int64:
        return strconv.FormatInt(t, 10)
    case uint:
        return strconv.FormatUint(uint64(t), 10)
    case uint8:
        return strconv.FormatUint(uint64(t), 10)
    case uint16:
        return strconv.FormatUint(uint64(t), 10)
    case uint32:
        return strconv.FormatUint(uint64(t), 10)
    case uint64:
        return strconv.FormatUint(t, 10)
    case float32:
        return strconv.FormatFloat(float64(t), 'f', -1, 32)
    case float64:
        return strconv.FormatFloat(t, 'f', -1, 64)
    case time.Time:
        return t.Format(time.RFC3339)
    case []byte:
        return string(t)
    default:
        return fmt.Sprint(t)
    }
}

// ExportCSV writes ResultSet rows as CSV to w. Column order is preserved.
func ExportCSV(w io.Writer, rs *engine.ResultSet, opts Options) error {
    csvw := csv.NewWriter(w)
    if opts.CSVDelimiter != 0 {
        csvw.Comma = opts.CSVDelimiter
    }
    if !opts.CSVNoHeader {
        if err := csvw.Write(rs.Cols); err != nil {
            return err
        }
    }
    for _, r := range rs.Rows {
        row := make([]string, len(rs.Cols))
        for i, c := range rs.Cols {
            row[i] = valueToString(r[strings.ToLower(c)])
        }
        if err := csvw.Write(row); err != nil {
            return err
        }
    }
    csvw.Flush()
    return csvw.Error()
}

// ExportJSON writes ResultSet rows as a JSON array of objects.
func ExportJSON(w io.Writer, rs *engine.ResultSet, opts Options) error {
    enc := json.NewEncoder(w)
    if opts.PrettyJSON {
        enc.SetIndent("", "  ")
    }
    // Convert rows to []map[string]any with display column names
    out := make([]map[string]any, len(rs.Rows))
    for i, r := range rs.Rows {
        m := make(map[string]any, len(rs.Cols))
        for _, c := range rs.Cols {
            m[c] = r[strings.ToLower(c)]
        }
        out[i] = m
    }
    return enc.Encode(out)
}

type xmlField struct {
    XMLName xml.Name
    Value   string `xml:",chardata"`
}

type xmlRow struct {
    Fields []xmlField `xml:",any"`
}

type xmlRows struct {
    XMLName xml.Name `xml:"rows"`
    Rows    []xmlRow  `xml:"row"`
}

// ExportXML writes ResultSet as simple XML: <rows><row><col>value</col>...</row>...</rows>
func ExportXML(w io.Writer, rs *engine.ResultSet) error {
    xr := xmlRows{XMLName: xml.Name{Local: "rows"}, Rows: make([]xmlRow, 0, len(rs.Rows))}
    for _, r := range rs.Rows {
        xrRow := xmlRow{Fields: make([]xmlField, 0, len(rs.Cols))}
        for _, c := range rs.Cols {
            xrRow.Fields = append(xrRow.Fields, xmlField{XMLName: xml.Name{Local: c}, Value: valueToString(r[strings.ToLower(c)])})
        }
        xr.Rows = append(xr.Rows, xrRow)
    }
    enc := xml.NewEncoder(w)
    enc.Indent("", "  ")
    if err := enc.Encode(xr); err != nil {
        return err
    }
    return enc.Flush()
}

// ExportGOB encodes the ResultSet.Rows using gob to w.
func ExportGOB(w io.Writer, rs *engine.ResultSet) error {
    enc := gob.NewEncoder(w)
    // Encode column order and rows; decoding side should expect the same shape.
    wrapper := struct {
        Cols []string
        Rows []map[string]any
    }{
        Cols: rs.Cols,
        Rows: make([]map[string]any, len(rs.Rows)),
    }
    for i, r := range rs.Rows {
        m := make(map[string]any, len(rs.Cols))
        for _, c := range rs.Cols {
            m[c] = r[strings.ToLower(c)]
        }
        wrapper.Rows[i] = m
    }
    return enc.Encode(wrapper)
}
