package exporter

import (
	"crypto/sha256"
	"encoding/base64"
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
	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func init() {
	// Register common concrete types stored in engine.Row (used as interface{}).
	gob.Register(time.Time{})
}

// Options controls exporter behavior.
type Options struct {
	PrettyJSON   bool
	CSVNoHeader  bool
	CSVDelimiter rune
	// BinaryEncoding controls CSV rendering of BLOB values. "base64" is the
	// default and writes a self-identifying base64: payload; "hex" writes a
	// self-identifying hex: payload. SQL always uses SQLite-compatible X'..'.
	BinaryEncoding string
	// JSONBinaryEnvelope preserves the BLOB/text distinction in JSON by
	// encoding BLOBs as {"$tinysql":"blob","base64":"..."}. It is enabled
	// by default; set JSONBinaryMode to "legacy-string" for a plain base64
	// string compatible with encoding/json's historical []byte behaviour.
	JSONBinaryMode string
}

// TableManifest is a portable, versioned description of an exported table.
// DataSHA256 fingerprints the ordered rows and typed cells without exposing
// their contents. It allows an import/export workflow to verify parity before
// publishing an artifact.
type TableManifest struct {
	FormatVersion int              `json:"format_version"`
	Tenant        string           `json:"tenant"`
	Table         string           `json:"table"`
	TextEncoding  string           `json:"text_encoding"`
	RowCount      int              `json:"row_count"`
	DataSHA256    string           `json:"data_sha256"`
	Columns       []ManifestColumn `json:"columns"`
}

// ManifestColumn preserves portable schema information without exposing
// tinySQL internal implementation fields.
type ManifestColumn struct {
	Name         string `json:"name"`
	DeclaredType string `json:"declared_type"`
	Affinity     string `json:"affinity,omitempty"`
	NotNull      bool   `json:"not_null"`
	HasDefault   bool   `json:"has_default"`
	DefaultSQL   string `json:"default_sql,omitempty"`
	Constraint   string `json:"constraint,omitempty"`
}

// ExportTableManifest writes a deterministic JSON schema and data fingerprint
// for one table. The corresponding data can be exported as CSV, JSON or SQL.
func ExportTableManifest(w io.Writer, db *storage.DB, tenant, tableName string) error {
	table, err := db.Get(tenant, tableName)
	if err != nil {
		return err
	}
	manifest := TableManifest{
		FormatVersion: 1,
		Tenant:        tenant,
		Table:         table.Name,
		TextEncoding:  "utf-8",
		RowCount:      len(table.Rows),
		DataSHA256:    tableDataSHA256(table.Rows),
		Columns:       make([]ManifestColumn, len(table.Cols)),
	}
	for i, col := range table.Cols {
		declared := col.DeclaredType
		if declared == "" {
			declared = col.Type.String()
		}
		manifest.Columns[i] = ManifestColumn{
			Name:         col.Name,
			DeclaredType: declared,
			Affinity:     col.Affinity.String(),
			NotNull:      col.NotNull || col.Constraint == storage.PrimaryKey,
			HasDefault:   col.HasDefault,
			DefaultSQL:   manifestDefaultSQL(col),
			Constraint:   col.Constraint.String(),
		}
	}
	return json.NewEncoder(w).Encode(manifest)
}

func manifestDefaultSQL(col storage.Column) string {
	if !col.HasDefault {
		return ""
	}
	return valueToSQLLiteral(col.DefaultValue)
}

func tableDataSHA256(rows [][]any) string {
	h := sha256.New()
	for _, row := range rows {
		for _, cell := range row {
			writeManifestCell(h, cell)
		}
		h.Write([]byte{0xff}) // row boundary distinct from all cell encodings
	}
	return fmt.Sprintf("%x", h.Sum(nil))
}

func writeManifestCell(w io.Writer, v any) {
	var kind, value string
	switch x := v.(type) {
	case nil:
		kind = "null"
	case []byte:
		kind, value = "blob", base64.StdEncoding.EncodeToString(x)
	case time.Time:
		kind, value = "time", x.UTC().Format(time.RFC3339Nano)
	default:
		kind, value = fmt.Sprintf("%T", v), fmt.Sprint(v)
	}
	fmt.Fprintf(w, "%s:%d:%s", kind, len(value), value)
	w.Write([]byte{0})
}

func valueToString(v any, binaryEncoding string) string {
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
		if strings.EqualFold(binaryEncoding, "hex") {
			return "hex:" + fmt.Sprintf("%x", t)
		}
		return "base64:" + base64.StdEncoding.EncodeToString(t)
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
			row[i] = valueToString(r[strings.ToLower(c)], opts.BinaryEncoding)
		}
		if err := csvw.Write(row); err != nil {
			return err
		}
	}
	csvw.Flush()
	return csvw.Error()
}

// jsonRowMap converts one ResultSet row into a map keyed by display column
// name, applying jsonValue's BLOB envelope handling per Options.
func jsonRowMap(r engine.Row, cols []string, opts Options) map[string]any {
	m := make(map[string]any, len(cols))
	for _, c := range cols {
		m[c] = jsonValue(r[strings.ToLower(c)], opts)
	}
	return m
}

// ExportJSON writes ResultSet rows as a JSON array of objects. Like
// ExportNDJSON, it encodes one row at a time instead of materializing a
// result-sized []map[string]any, so memory use stays proportional to a
// single row rather than the whole result set. The bytes written are
// identical to json.Encoder (with the same PrettyJSON setting) encoding that
// materialized slice in one call.
func ExportJSON(w io.Writer, rs *engine.ResultSet, opts Options) error {
	if _, err := io.WriteString(w, "["); err != nil {
		return err
	}
	for i, r := range rs.Rows {
		m := jsonRowMap(r, rs.Cols, opts)
		var b []byte
		var err error
		if opts.PrettyJSON {
			b, err = json.MarshalIndent(m, "  ", "  ")
		} else {
			b, err = json.Marshal(m)
		}
		if err != nil {
			return err
		}
		sep := ""
		if i > 0 {
			sep = ","
		}
		if opts.PrettyJSON {
			sep += "\n  "
		}
		if _, err := io.WriteString(w, sep); err != nil {
			return err
		}
		if _, err := w.Write(b); err != nil {
			return err
		}
	}
	if opts.PrettyJSON && len(rs.Rows) > 0 {
		if _, err := io.WriteString(w, "\n"); err != nil {
			return err
		}
	}
	_, err := io.WriteString(w, "]\n")
	return err
}

// ExportNDJSON writes one JSON object per ResultSet row. Unlike ExportJSON it
// does not materialize a result-sized array, so it is suitable for pipes and
// large exports. Each line is independently valid JSON.
func ExportNDJSON(w io.Writer, rs *engine.ResultSet, opts Options) error {
	enc := json.NewEncoder(w)
	for _, r := range rs.Rows {
		m := jsonRowMap(r, rs.Cols, opts)
		if err := enc.Encode(m); err != nil {
			return err
		}
	}
	return nil
}

func jsonValue(v any, opts Options) any {
	b, ok := v.([]byte)
	if !ok || strings.EqualFold(opts.JSONBinaryMode, "legacy-string") {
		return v
	}
	return map[string]string{
		"$tinysql": "blob",
		"base64":   base64.StdEncoding.EncodeToString(b),
	}
}

// ExportSQL writes ResultSet rows as INSERT statements for tableName.
func ExportSQL(w io.Writer, rs *engine.ResultSet, tableName string) error {
	for _, r := range rs.Rows {
		values := make([]string, len(rs.Cols))
		for i, c := range rs.Cols {
			values[i] = valueToSQLLiteral(r[strings.ToLower(c)])
		}
		if _, err := fmt.Fprintf(w, "INSERT INTO %s (%s) VALUES (%s);\n",
			tableName, strings.Join(rs.Cols, ", "), strings.Join(values, ", ")); err != nil {
			return err
		}
	}
	return nil
}

func valueToSQLLiteral(v any) string {
	if v == nil {
		return "NULL"
	}
	switch t := v.(type) {
	case string:
		return "'" + strings.ReplaceAll(t, "'", "''") + "'"
	case time.Time:
		return "'" + t.Format(time.RFC3339) + "'"
	case []byte:
		return "X'" + fmt.Sprintf("%X", t) + "'"
	default:
		return valueToString(v, "base64")
	}
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
	Rows    []xmlRow `xml:"row"`
}

// ExportXML writes ResultSet as simple XML: <rows><row><col>value</col>...</row>...</rows>
func ExportXML(w io.Writer, rs *engine.ResultSet) error {
	xr := xmlRows{XMLName: xml.Name{Local: "rows"}, Rows: make([]xmlRow, 0, len(rs.Rows))}
	for _, r := range rs.Rows {
		xrRow := xmlRow{Fields: make([]xmlField, 0, len(rs.Cols))}
		for _, c := range rs.Cols {
			xrRow.Fields = append(xrRow.Fields, xmlField{XMLName: xml.Name{Local: c}, Value: valueToString(r[strings.ToLower(c)], "base64")})
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
