// Result rows handed back to database/sql, and the column metadata it can ask
// for.
package driver

import (
	"database/sql/driver"
	"fmt"
	"io"
	"reflect"
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

	// onClose owns resources that must outlive a streamed result: the server's
	// reader slot and, for a prepared SELECT, its borrowed AST. The server
	// RLock is released once ExecuteStream establishes its storage snapshot.
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
		row := r.stream.Row()
		err := r.copyRow(row, r.streamCols, dest)
		// copyRow has already read every value it needs out of row into dest;
		// nothing else can still reference this specific map (see
		// ResultStream.ReleaseRow's doc comment), so hand it back for reuse.
		r.stream.ReleaseRow(row)
		return err
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
		// Parser-produced output names are overwhelmingly already lowercase.
		// Reuse Cols itself in that common case and allocate a separate key slice
		// only after encountering a name that actually needs case folding.
		r.lowerCols = cols
		copied := false
		for i, c := range cols {
			lower := strings.ToLower(c)
			if lower == c {
				continue
			}
			if !copied {
				r.lowerCols = append([]string(nil), cols...)
				copied = true
			}
			r.lowerCols[i] = lower
		}
		r.cachedRS = r.rs
	}
	for i := range cols {
		v := row[r.lowerCols[i]]
		switch vv := v.(type) {
		case nil, int64, float64, bool, string, time.Time:
			// These values already have a database/sql-supported dynamic type.
			// Preserve the existing interface box: assigning the type-switch
			// variable would box numeric values again and allocate per cell.
			dest[i] = v
		case int:
			dest[i] = int64(vv)
		case []byte:
			// database/sql callers may retain Scan destinations; return an owned
			// slice just as the standard drivers do for binary columns.
			dest[i] = append([]byte(nil), vv...)
		default:
			b, err := storage.JSONMarshal(vv)
			if err != nil {
				return fmt.Errorf("tinysql: encode column %q: %w", cols[i], err)
			}
			dest[i] = string(b)
		}
	}
	return nil
}

// Optional ColumnType* (informativ)
func (r *rows) ColumnTypeDatabaseTypeName(i int) string { return "TEXT" }

func (r *rows) ColumnTypeNullable(i int) (bool, bool) { return true, true }

func (r *rows) ColumnTypeScanType(i int) reflect.Type { return reflect.TypeFor[any]() }

var (
	_ driver.RowsColumnTypeScanType = (*rows)(nil)
	_ driver.RowsColumnTypeScanType = emptyRows{}
)

type emptyRows struct{}

func (emptyRows) Columns() []string { return []string{} }

func (emptyRows) Close() error { return nil }

func (emptyRows) Next([]driver.Value) error { return io.EOF }

func (emptyRows) ColumnTypeDatabaseTypeName(int) string { return "TEXT" }

func (emptyRows) ColumnTypeNullable(int) (bool, bool) { return true, true }

func (emptyRows) ColumnTypeScanType(int) reflect.Type { return reflect.TypeFor[any]() }
