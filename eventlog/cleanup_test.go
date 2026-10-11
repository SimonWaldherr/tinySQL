package eventlog

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"testing"
)

type rollbackConnector struct{ err error }

func (c rollbackConnector) Connect(context.Context) (driver.Conn, error) {
	return rollbackConn{c.err}, nil
}
func (c rollbackConnector) Driver() driver.Driver { return rollbackDriver{} }

type rollbackDriver struct{}

func (rollbackDriver) Open(string) (driver.Conn, error) {
	return nil, errors.New("use connector")
}

type rollbackConn struct{ err error }

func (rollbackConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("prepare unsupported")
}
func (rollbackConn) Close() error { return nil }
func (c rollbackConn) Begin() (driver.Tx, error) {
	return rollbackTx{c.err}, nil
}

type rollbackTx struct{ err error }

func (rollbackTx) Commit() error      { return nil }
func (tx rollbackTx) Rollback() error { return tx.err }

func TestRollbackPreservesErrors(t *testing.T) {
	primary := errors.New("query failed")
	cleanup := errors.New("rollback failed")
	for _, committed := range []bool{false, true} {
		for _, prior := range []error{nil, primary} {
			db := sql.OpenDB(rollbackConnector{cleanup})
			t.Cleanup(func() { db.Close() })
			tx, err := db.BeginTx(t.Context(), nil)
			if err != nil {
				t.Fatal(err)
			}
			if committed {
				if err := tx.Commit(); err != nil {
					t.Fatal(err)
				}
			}
			err = prior
			rollback(tx, &err)
			if committed && err != prior {
				t.Fatalf("cleanup after commit changed result: %v", err)
			}
			if !committed && !errors.Is(err, cleanup) {
				t.Fatalf("lost rollback error: %v", err)
			}
			if prior != nil && !errors.Is(err, prior) {
				t.Fatalf("lost query error: %v", err)
			}
		}
	}
}
