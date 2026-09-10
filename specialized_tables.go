package tinysql

import (
	"context"
	"fmt"

	"github.com/SimonWaldherr/tinySQL/internal/engine"
)

// TableKind selects a schema/index profile, independently of the DB storage mode.
type TableKind string

const (
	// KeyValueTable uses a TEXT primary key and a nullable BLOB value.
	KeyValueTable TableKind = "keyvalue"
	// DocumentTable uses a TEXT primary key and a non-null JSON document.
	DocumentTable TableKind = "document"
	// TimeSeriesTable uses series TEXT, time INT, value FLOAT and a series/time index.
	TimeSeriesTable TableKind = "timeseries"
)

// CreateSpecializedTable creates a SQL-compatible table. Optional columns rename
// the profile's columns in order; either all names or none must be supplied.
// Duplicate table names return an error. Use ordinary SQL for reads and writes.
func CreateSpecializedTable(ctx context.Context, db *DB, tenant, name string, kind TableKind, columns ...string) error {
	if db == nil || name == "" {
		return fmt.Errorf("database and table name are required")
	}
	switch kind {
	case KeyValueTable, DocumentTable, TimeSeriesTable:
	default:
		return fmt.Errorf("unknown table kind %q", kind)
	}
	_, err := Execute(ctx, db, tenant, &engine.CreateTable{Name: name, VirtualTable: true, Using: string(kind), FTSColumns: append([]string(nil), columns...)})
	return err
}
