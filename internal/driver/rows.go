// Result rows handed back to database/sql, and the column metadata it can ask
// for.
package driver

import (
	"database/sql/driver"
	"io"
	"strings"
	"time"

	"github.com/SimonWaldherr/tinySQL/internal/engine"
	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

type rows struct {
	rs        *engine.ResultSet
	cachedRS  *engine.ResultSet
	lowerCols []string
	i         int
}

func (r *rows) Columns() []string { return r.rs.Cols }

func (r *rows) Close() error { return nil }

func (r *rows) Next(dest []driver.Value) error {
	if r.i >= len(r.rs.Rows) {
		return io.EOF
	}
	if r.cachedRS != r.rs || len(r.lowerCols) != len(r.rs.Cols) {
		r.lowerCols = make([]string, len(r.rs.Cols))
		for i, c := range r.rs.Cols {
			r.lowerCols[i] = strings.ToLower(c)
		}
		r.cachedRS = r.rs
	}
	row := r.rs.Rows[r.i]
	for i := range r.rs.Cols {
		v := row[r.lowerCols[i]]
		switch vv := v.(type) {
		case nil:
			dest[i] = nil
		case int:
			dest[i] = int64(vv)
		case int64:
			dest[i] = vv
		case float64:
			dest[i] = vv
		case bool:
			dest[i] = vv
		case string:
			dest[i] = vv
		case time.Time:
			// RFC3339Nano to match the bind path (CheckNamedValue), so sub-second
			// precision survives a bind -> store -> scan round trip.
			dest[i] = vv.Format(time.RFC3339Nano)
		case []byte:
			// database/sql callers may retain Scan destinations; return an owned
			// slice just as the standard drivers do for binary columns.
			dest[i] = append([]byte(nil), vv...)
		default:
			b, _ := storage.JSONMarshal(vv)
			dest[i] = string(b)
		}
	}
	r.i++
	return nil
}

// Optional ColumnType* (informativ)
func (r *rows) ColumnTypeDatabaseTypeName(i int) string { return "TEXT" }

func (r *rows) ColumnTypeNullable(i int) (bool, bool) { return true, true }

func (r *rows) ColumnTypeScanType(i int) any { return "interface{}" }

type emptyRows struct{}

func (emptyRows) Columns() []string { return []string{} }

func (emptyRows) Close() error { return nil }

func (emptyRows) Next([]driver.Value) error { return io.EOF }

func (emptyRows) ColumnTypeDatabaseTypeName(int) string { return "TEXT" }

func (emptyRows) ColumnTypeNullable(int) (bool, bool) { return true, true }

func (emptyRows) ColumnTypeScanType(int) any { return "interface{}" }
