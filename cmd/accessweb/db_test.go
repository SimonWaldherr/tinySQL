package main

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"strings"
	"testing"
)

type closeErrorRows struct {
	closeErr error
	nextErr  error
	read     bool
	closes   int
}

func (r *closeErrorRows) Columns() []string { return []string{"id"} }
func (r *closeErrorRows) Close() error {
	r.closes++
	return r.closeErr
}
func (r *closeErrorRows) Next(dest []driver.Value) error {
	if r.nextErr != nil {
		return r.nextErr
	}
	if r.read {
		return io.EOF
	}
	r.read = true
	dest[0] = int64(1)
	return nil
}

type closeErrorConnector struct{ rows *closeErrorRows }

func (c closeErrorConnector) Connect(context.Context) (driver.Conn, error) {
	return closeErrorConn{c.rows}, nil
}
func (c closeErrorConnector) Driver() driver.Driver { return closeErrorDriver{} }

type closeErrorDriver struct{}

func (closeErrorDriver) Open(string) (driver.Conn, error) {
	return nil, errors.New("use connector")
}

type closeErrorConn struct{ rows *closeErrorRows }

func (closeErrorConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("prepare unsupported")
}
func (closeErrorConn) Close() error { return nil }
func (closeErrorConn) Begin() (driver.Tx, error) {
	return nil, errors.New("transactions unsupported")
}
func (c closeErrorConn) QueryContext(context.Context, string, []driver.NamedValue) (driver.Rows, error) {
	return c.rows, nil
}

func newCloseErrorDB(t *testing.T, rows *closeErrorRows) *sql.DB {
	t.Helper()
	db := sql.OpenDB(closeErrorConnector{rows})
	t.Cleanup(func() { db.Close() })
	return db
}

func TestCloseRowsPreservesErrors(t *testing.T) {
	closeErr := errors.New("stream close failed")
	queryErr := errors.New("query failed")
	for _, prior := range []error{nil, queryErr} {
		t.Run("prior="+anyToString(prior), func(t *testing.T) {
			stream := &closeErrorRows{closeErr: closeErr}
			rows, err := newCloseErrorDB(t, stream).QueryContext(context.Background(), "SELECT id")
			if err != nil {
				t.Fatal(err)
			}
			err = prior
			closeRows(rows, &err)
			if !errors.Is(err, closeErr) || !strings.Contains(err.Error(), "close query rows:") {
				t.Fatalf("expected wrapped close error, got %v", err)
			}
			if prior != nil && !errors.Is(err, prior) {
				t.Fatalf("lost query error: %v", err)
			}
			if stream.closes != 1 {
				t.Fatalf("expected one close, got %d", stream.closes)
			}
		})
	}
}

func TestQueryFunctionsReportCloseErrors(t *testing.T) {
	closeErr := errors.New("stream close failed")
	for _, method := range []string{"tableRows", "getRecord", "queryRows"} {
		t.Run(method, func(t *testing.T) {
			stream := &closeErrorRows{closeErr: closeErr}
			app := &App{sqlDB: newCloseErrorDB(t, stream)}
			var err error
			switch method {
			case "tableRows":
				_, _, err = app.tableRows(context.Background(), "items", 1)
			case "getRecord":
				_, _, err = app.getRecord(context.Background(), "items", "1")
			case "queryRows":
				_, _, err = app.queryRows(context.Background(), "SELECT id FROM items")
			}
			if !errors.Is(err, closeErr) {
				t.Fatalf("expected close error, got %v", err)
			}
			if stream.closes != 1 {
				t.Fatalf("expected one close, got %d", stream.closes)
			}
		})
	}
}

func TestGetRecordPreservesIterationError(t *testing.T) {
	nextErr := errors.New("stream read failed")
	stream := &closeErrorRows{nextErr: nextErr}
	app := &App{sqlDB: newCloseErrorDB(t, stream)}
	_, _, err := app.getRecord(context.Background(), "items", "1")
	if !errors.Is(err, nextErr) {
		t.Fatalf("expected iteration error, got %v", err)
	}
}
