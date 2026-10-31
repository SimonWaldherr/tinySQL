package tinysql

import (
	"github.com/SimonWaldherr/tinySQL/internal/engine"
	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// DB represents a multi-tenant database instance with support for MVCC and WAL.
// Use NewDB to create a new instance.
type DB = storage.DB

// Table represents a database table with columns and rows.
// Tables are created via CREATE TABLE statements and accessed through the DB.
type Table = storage.Table

// Column represents a table column with a name and type.
type Column = storage.Column

// ColType enumerates supported column data types (INT, TEXT, BOOL, JSON, etc.).
type ColType = storage.ColType

// ConstraintType enumerates supported column constraints.
type ConstraintType = storage.ConstraintType

// ColumnStats and TableStats are the persisted results of an ANALYZE command.
type ColumnStats = storage.ColumnStats

type TableStats = storage.TableStats

// APIVersion is the compatibility contract for tinySQL's root Go package.
// Additive APIs retain this value; an incompatible public API change requires
// a new major module version and a new value.
const APIVersion = 1

// Version returns the module version embedded by Go's build metadata, or
// "dev" when the binary/package was built outside a tagged module version.
func Version() string { return engine.Version() }

const (
	NoConstraint ConstraintType = storage.NoConstraint
	PrimaryKey   ConstraintType = storage.PrimaryKey
	Unique       ConstraintType = storage.Unique
)

const (
	// IntType represents a generic integer column type.
	IntType ColType = storage.IntType
	// Int8Type represents an 8-bit signed integer column type.
	Int8Type ColType = storage.Int8Type
	// Int16Type represents a 16-bit signed integer column type.
	Int16Type ColType = storage.Int16Type
	// Int32Type represents a 32-bit signed integer column type.
	Int32Type ColType = storage.Int32Type
	// Int64Type represents a 64-bit signed integer column type.
	Int64Type ColType = storage.Int64Type
	// UintType represents an unsigned integer column type.
	UintType ColType = storage.UintType
	// Uint8Type represents an 8-bit unsigned integer column type.
	Uint8Type ColType = storage.Uint8Type

	// Float32Type represents a 32-bit floating point column type.
	Float32Type ColType = storage.Float32Type
	// Float64Type represents a 64-bit floating point column type.
	Float64Type ColType = storage.Float64Type
	// FloatType is an alias for Float64Type.
	FloatType ColType = storage.FloatType

	// StringType represents a short string/text column.
	StringType ColType = storage.StringType
	// TextType represents a longer textual column.
	TextType ColType = storage.TextType

	// BoolType represents a boolean column (true/false).
	BoolType ColType = storage.BoolType

	// TimeType represents a time-only column type.
	TimeType ColType = storage.TimeType
	// DateType represents a date-only column type.
	DateType ColType = storage.DateType
	// DateTimeType represents a combined date and time column.
	DateTimeType ColType = storage.DateTimeType
	// TimestampType represents a timestamp column type.
	TimestampType ColType = storage.TimestampType

	// JsonType represents a JSON textual column.
	JsonType ColType = storage.JsonType
	// JsonbType represents a binary JSON column (if supported by backend).
	JsonbType ColType = storage.JsonbType
	// GeometryType represents spatial geometry values (GeoJSON/WKB).
	GeometryType ColType = storage.GeometryType
	// Decimal and money types
	DecimalType  ColType = storage.DecimalType
	MoneyType    ColType = storage.MoneyType
	UUIDType     ColType = storage.UUIDType
	BlobType     ColType = storage.BlobType
	XMLType      ColType = storage.XMLType
	IntervalType ColType = storage.IntervalType
)

// NewDB creates a new in-memory multi-tenant database instance with MVCC support.
//
// The database starts empty with no tables. Use SQL DDL statements to create tables,
// or load from a file with LoadFromFile.
//
// Example:
//
//	db := tinysql.NewDB()
//	defer db.Close() // Optional cleanup
//
// The returned DB is safe for concurrent use and includes an integrated MVCC manager.
func NewDB() *DB {
	return storage.NewDB()
}

// NewTable creates a new table with the specified columns.
//
// This is a low-level API. Normally tables are created via CREATE TABLE statements.
// Use this when programmatically building table structures.
//
// Example:
//
//	cols := []tinysql.Column{
//	    {Name: "id", Type: tinysql.IntType},
//	    {Name: "name", Type: tinysql.TextType},
//	}
//	table := tinysql.NewTable("users", cols, false)
//	db.Put("default", table)
//
// The isTemp parameter indicates if this is a temporary table (not persisted).
func NewTable(name string, cols []Column, isTemp bool) *Table {
	return storage.NewTable(name, cols, isTemp)
}
