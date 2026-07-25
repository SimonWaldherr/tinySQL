// The column type system: storage types, SQLite type affinities, column
// constraints and referential actions. These are the vocabulary the schema is
// written in, so they are kept apart from the database that stores it.
package storage

import (
	"encoding/gob"
	"fmt"
	"strings"
)

// safeGobRegister registers a type with encoding/gob but recovers from the
// known "registering duplicate names" panic which can occur when the same
// type is registered via different import paths in a multi-package build
// (for example when using Wails and building bindings). Ignoring that
// specific panic is safe for our use-case.
func safeGobRegister(v any) {
	defer func() {
		if r := recover(); r != nil {
			// If the panic is the duplicate registration panic from gob,
			// ignore it. Otherwise re-panic to avoid hiding real problems.
			if strings.Contains(fmt.Sprint(r), "registering duplicate names") {
				return
			}
			panic(r)
		}
	}()
	gob.Register(v)
}

// ColType enumerates supported column data types.
type ColType int

const (
	// IntType is a generic integer column type.
	IntType ColType = iota
	// Int8Type is an 8-bit signed integer column type.
	Int8Type
	// Int16Type is a 16-bit signed integer column type.
	Int16Type
	// Int32Type is a 32-bit signed integer column type.
	Int32Type
	// Int64Type is a 64-bit signed integer column type.
	Int64Type
	// UintType is an unsigned integer column type.
	UintType
	// Uint8Type is an 8-bit unsigned integer column type.
	Uint8Type
	// Uint16Type is a 16-bit unsigned integer column type.
	Uint16Type
	// Uint32Type is a 32-bit unsigned integer column type.
	Uint32Type
	// Uint64Type is a 64-bit unsigned integer column type.
	Uint64Type

	// Float32Type is a 32-bit floating point column type.
	Float32Type
	// Float64Type is a 64-bit floating point column type.
	Float64Type
	// FloatType is an alias for Float64Type.
	FloatType // alias for Float64Type

	// StringType represents a variable-length UTF-8 string column.
	StringType
	// TextType is an alias for StringType intended for long text.
	TextType // alias for StringType
	// RuneType stores single Unicode code points.
	RuneType
	// ByteType stores raw byte data.
	ByteType

	// BoolType represents a boolean column (true/false).
	BoolType

	// TimeType stores time-of-day values.
	TimeType
	// DateType stores date-only values.
	DateType
	// DateTimeType stores combined date and time values.
	DateTimeType
	// TimestampType stores an absolute point in time.
	TimestampType
	// DurationType stores a time duration.
	DurationType

	// DecimalType stores arbitrary-precision decimal numbers.
	DecimalType
	// MoneyType is a convenience alias for DecimalType used for monetary values.
	MoneyType
	// UUIDType stores RFC-4122 UUID values.
	UUIDType
	// BlobType stores binary large objects.
	BlobType
	// XMLType stores XML text.
	XMLType
	// IntervalType stores SQL-like intervals (parsed to time.Duration when possible).
	IntervalType

	// JsonType stores JSON text.
	JsonType
	// JsonbType stores binary JSON representations.
	JsonbType
	// MapType stores map-like complex values.
	MapType
	// SliceType stores slice-like complex values.
	SliceType
	// ArrayType stores array-like complex values.
	ArrayType

	// Complex64Type stores complex64 numeric values.
	Complex64Type
	// Complex128Type stores complex128 numeric values.
	Complex128Type
	// ComplexType is an alias for Complex128Type.
	ComplexType // alias for Complex128Type
	// PointerType represents a pointer/reference to another object.
	PointerType
	// InterfaceType represents an arbitrary Go interface value.
	InterfaceType

	// VectorType represents a vector/embedding column used by RAG features.
	VectorType
	// GeometryType stores spatial geometry values (GeoJSON/WKB) as JSONB or binary payload.
	GeometryType

	// YAMLType stores YAML-formatted text data.
	YAMLType
	// URLType stores URL/URI values with optional validation.
	URLType
	// HASHType stores cryptographic hash digests (hex-encoded).
	HASHType
	// BitmapType stores roaring-bitmap or bitset values as a byte slice.
	BitmapType
)

var colTypeToString = map[ColType]string{
	IntType:        "INT",
	Int8Type:       "INT8",
	Int16Type:      "INT16",
	Int32Type:      "INT32",
	Int64Type:      "INT64",
	UintType:       "UINT",
	Uint8Type:      "UINT8",
	Uint16Type:     "UINT16",
	Uint32Type:     "UINT32",
	Uint64Type:     "UINT64",
	Float32Type:    "FLOAT32",
	Float64Type:    "FLOAT64",
	FloatType:      "FLOAT64",
	StringType:     "STRING",
	TextType:       "TEXT",
	RuneType:       "RUNE",
	ByteType:       "BYTE",
	BoolType:       "BOOL",
	TimeType:       "TIME",
	DateType:       "DATE",
	DateTimeType:   "DATETIME",
	TimestampType:  "TIMESTAMP",
	DurationType:   "DURATION",
	JsonType:       "JSON",
	JsonbType:      "JSONB",
	MapType:        "MAP",
	SliceType:      "SLICE",
	ArrayType:      "ARRAY",
	Complex64Type:  "COMPLEX64",
	Complex128Type: "COMPLEX",
	ComplexType:    "COMPLEX",
	PointerType:    "POINTER",
	InterfaceType:  "INTERFACE",
	VectorType:     "VECTOR",
	GeometryType:   "GEOMETRY",
	YAMLType:       "YAML",
	URLType:        "URL",
	HASHType:       "HASH",
	BitmapType:     "BITMAP",
	// Additional types
	DecimalType:  "DECIMAL",
	MoneyType:    "MONEY",
	UUIDType:     "UUID",
	BlobType:     "BLOB",
	XMLType:      "XML",
	IntervalType: "INTERVAL",
}

// SQLiteAffinity is the five-class type system used by SQLite declarations.
// It is schema metadata, not another runtime value type: tinySQL continues to
// store NULL, integer, real, text and binary values directly. Keeping the
// declared affinity separate lets imported SQLite schemas retain their
// lossless-coercion behaviour without multiplying ColType values.
type SQLiteAffinity uint8

const (
	// AffinityDefault retains tinySQL's native, strongly typed coercion rules.
	AffinityDefault SQLiteAffinity = iota
	AffinityInteger
	AffinityText
	AffinityNumeric
	AffinityReal
	AffinityBlob
)

func (a SQLiteAffinity) String() string {
	switch a {
	case AffinityInteger:
		return "INTEGER"
	case AffinityText:
		return "TEXT"
	case AffinityNumeric:
		return "NUMERIC"
	case AffinityReal:
		return "REAL"
	case AffinityBlob:
		return "BLOB"
	default:
		return ""
	}
}

func (t ColType) String() string {
	if s, ok := colTypeToString[t]; ok {
		return s
	}
	return "UNKNOWN"
}

// ConstraintType enumerates supported column constraints.
type ConstraintType int

const (
	NoConstraint ConstraintType = iota
	PrimaryKey
	ForeignKey
	Unique
)

func (c ConstraintType) String() string {
	switch c {
	case PrimaryKey:
		return "PRIMARY KEY"
	case ForeignKey:
		return "FOREIGN KEY"
	case Unique:
		return "UNIQUE"
	default:
		return ""
	}
}

// ReferentialAction enumerates what happens to a child row when the parent
// row it references is deleted (ON DELETE) or its referenced column value
// changes (ON UPDATE).
type ReferentialAction int

const (
	// NoAction means no ON DELETE/ON UPDATE clause was given. tinySQL checks
	// foreign key constraints immediately (it has no deferred-constraint
	// mode), so NoAction and Restrict behave identically here — the SQL
	// standard only distinguishes them for deferred checking.
	NoAction ReferentialAction = iota
	// Restrict blocks the parent-side mutation while a referencing child row exists.
	Restrict
	// Cascade propagates the parent-side delete/update to matching child rows.
	Cascade
	// SetNull nulls the child row's foreign key column instead of deleting/blocking.
	SetNull
)

func (a ReferentialAction) String() string {
	switch a {
	case Restrict:
		return "RESTRICT"
	case Cascade:
		return "CASCADE"
	case SetNull:
		return "SET NULL"
	default:
		return "NO ACTION"
	}
}

// ForeignKeyRef describes a foreign key reference target.
type ForeignKeyRef struct {
	Table    string
	Column   string
	OnDelete ReferentialAction
	OnUpdate ReferentialAction
}

// Column holds column schema information in a table.
type Column struct {
	Name string
	Type ColType
	// DeclaredType retains the source SQL spelling (for example VARCHAR(80)
	// or DOUBLE PRECISION). It is intentionally metadata; the physical value
	// representation remains ColType plus SQLiteAffinity.
	DeclaredType string
	// Affinity is populated for SQLite-style declarations. AffinityDefault
	// means this column was declared using a native tinySQL type.
	Affinity SQLiteAffinity
	// NotNull and default metadata are independent of Constraint because SQL
	// permits combinations such as "PRIMARY KEY NOT NULL DEFAULT 0".
	NotNull      bool
	HasDefault   bool
	DefaultValue any
	Constraint   ConstraintType
	ForeignKey   *ForeignKeyRef // Only used if Constraint == ForeignKey
	PointerTable string         // Target table for POINTER type
}
