// Result rows handed back to database/sql, and the column metadata it can ask
// for.
package driver

import (
	"database/sql/driver"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/SimonWaldherr/tinySQL/internal/engine"
	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

type rows struct {
	rs         *engine.ResultSet
	stream     *engine.ResultStream
	streamCols []string
	cachedRS   *engine.ResultSet
	lowerCols  []string
	i          int

	// onClose owns resources that must outlive a streamed result: the
	// server's reader slot/RLock and, for a prepared SELECT, its borrowed AST.
	// It is intentionally run only after ResultStream.Close has waited for its
	// producer, so neither the database nor a pooled prepared execution can be
	// reused while the producer still references it.
	onClose   func()
	closeOnce sync.Once
	closeErr  error
	closed    atomic.Bool
}

func (r *rows) Columns() []string {
	if r.stream != nil {
		return r.streamCols
	}
	return r.rs.Cols
}

func (r *rows) Close() error {
	r.closed.Store(true)
	r.closeOnce.Do(func() {
		if r.stream != nil {
			r.closeErr = r.stream.Close()
		}
		if r.onClose != nil {
			r.onClose()
		}
	})
	return r.closeErr
}

func (r *rows) Next(dest []driver.Value) error {
	if r.closed.Load() {
		if err := r.Close(); err != nil {
			return err
		}
		return io.EOF
	}

	if r.stream != nil {
		if !r.stream.Next() {
			err := r.stream.Err()
			if closeErr := r.Close(); err == nil {
				err = closeErr
			}
			if err != nil {
				return err
			}
			return io.EOF
		}
		if r.closed.Load() {
			if err := r.Close(); err != nil {
				return err
			}
			return io.EOF
		}
		return r.copyRow(r.stream.Row(), r.streamCols, dest)
	}

	if r.i >= len(r.rs.Rows) {
		if err := r.Close(); err != nil {
			return err
		}
		return io.EOF
	}
	if err := r.copyRow(r.rs.Rows[r.i], r.rs.Cols, dest); err != nil {
		return err
	}
	r.i++
	return nil
}

func (r *rows) copyRow(row engine.Row, cols []string, dest []driver.Value) error {
	if r.cachedRS != r.rs || len(r.lowerCols) != len(cols) {
		r.lowerCols = make([]string, len(cols))
		for i, c := range cols {
			r.lowerCols[i] = strings.ToLower(c)
		}
		r.cachedRS = r.rs
	}
	for i := range cols {
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
