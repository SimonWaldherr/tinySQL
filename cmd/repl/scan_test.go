package main

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"os"
	"reflect"
	"strings"
	"testing"
)

// This driver reuses its byte buffer on every Next and invalidates it on Close.
// Scanned results must own their values after the row cursor advances.
type replScanConnector struct{ terminal error }
type replScanDriver struct{}
type replScanConn struct{ terminal error }
type replScanRows struct {
	terminal error
	index    int
	buffer   [1]byte
}

func (replScanDriver) Open(string) (driver.Conn, error) { return nil, errors.New("use connector") }
func (c replScanConnector) Driver() driver.Driver       { return replScanDriver{} }
func (c replScanConnector) Connect(context.Context) (driver.Conn, error) {
	return replScanConn{terminal: c.terminal}, nil
}
func (replScanConn) Prepare(string) (driver.Stmt, error) { return nil, errors.New("unsupported") }
func (replScanConn) Begin() (driver.Tx, error)           { return nil, errors.New("unsupported") }
func (replScanConn) Close() error                        { return nil }
func (c replScanConn) QueryContext(context.Context, string, []driver.NamedValue) (driver.Rows, error) {
	return &replScanRows{terminal: c.terminal}, nil
}
func (*replScanRows) Columns() []string { return []string{"id", "payload", "optional"} }
func (r *replScanRows) Close() error    { r.buffer[0] = 'x'; return nil }
func (r *replScanRows) Next(dest []driver.Value) error {
	if r.index == 2 {
		if r.terminal != nil {
			return r.terminal
		}
		return io.EOF
	}
	r.buffer[0] = byte('a' + r.index)
	dest[0], dest[1], dest[2] = int64(r.index), r.buffer[:], nil
	if r.index == 0 {
		dest[2] = "first"
	}
	r.index++
	return nil
}

func TestRowsToSliceOwnsScannedValues(t *testing.T) {
	db := sql.OpenDB(replScanConnector{})
	defer db.Close()
	rows, err := db.Query("fixture")
	if err != nil {
		t.Fatal(err)
	}
	cols, err := rows.Columns()
	if err != nil {
		t.Fatal(err)
	}
	got, err := rowsToSlice(rows, cols)
	rows.Close()
	if err != nil {
		t.Fatal(err)
	}
	want := []map[string]any{
		{"id": int64(0), "payload": []byte("a"), "optional": "first"},
		{"id": int64(1), "payload": []byte("b"), "optional": nil},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("rows after cursor close: %#v, want %#v", got, want)
	}
}

func TestRowsToSliceReportsIterationError(t *testing.T) {
	failure := errors.New("read failed after partial results")
	db := sql.OpenDB(replScanConnector{terminal: failure})
	defer db.Close()
	rows, err := db.Query("fixture")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		t.Fatal(err)
	}
	_, err = rowsToSlice(rows, cols)
	if !errors.Is(err, failure) {
		t.Fatalf("error=%v, want %v", err, failure)
	}
}

func TestErrorsOnlyReportsLazyErrors(t *testing.T) {
	for _, htmlMode := range []bool{false, true} {
		for _, fails := range []bool{false, true} {
			t.Run(fmt.Sprintf("html%t/failure%t", htmlMode, fails), func(t *testing.T) {
				var failure error
				if fails {
					failure = errors.New("read failed after partial results")
				}
				db := sql.OpenDB(replScanConnector{terminal: failure})
				defer db.Close()
				file, err := os.CreateTemp(t.TempDir(), "output")
				if err != nil {
					t.Fatal(err)
				}
				defer file.Close()
				previous := os.Stdout
				os.Stdout = file
				defer func() { os.Stdout = previous }()
				var parts []string
				err = handleSelectStatement(db, "SELECT 1", false, htmlMode, true, "table", nil, &parts, false)
				if !errors.Is(err, failure) {
					t.Fatalf("error=%v, want %v", err, failure)
				}
				output, readErr := os.ReadFile(file.Name())
				if readErr != nil {
					t.Fatal(readErr)
				}
				if htmlMode {
					if len(output) != 0 {
						t.Fatalf("HTML wrote to stdout: %s", output)
					}
					output = []byte(strings.Join(parts, ""))
				}
				if fails {
					if !strings.Contains(string(output), failure.Error()) {
						t.Fatalf("missing lazy error: %s", output)
					}
				} else if len(output) != 0 {
					t.Fatalf("successful errors-only query produced output: %s", output)
				}
			})
		}
	}
}
