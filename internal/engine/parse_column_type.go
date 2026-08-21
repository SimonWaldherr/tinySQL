// Parsing a declared column type and mapping it to a storage type and a SQLite
// type affinity.
package engine

import (
	"strings"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

var typeKeywordMap = map[string]storage.ColType{
	// Integer types
	"INT":    storage.IntType,
	"INT8":   storage.Int8Type,
	"INT16":  storage.Int16Type,
	"INT32":  storage.Int32Type,
	"INT64":  storage.Int64Type,
	"UINT":   storage.UintType,
	"UINT8":  storage.Uint8Type,
	"UINT16": storage.Uint16Type,
	"UINT32": storage.Uint32Type,
	"UINT64": storage.Uint64Type,
	// Floating point types
	"FLOAT":   storage.Float64Type,
	"FLOAT64": storage.Float64Type,
	"DOUBLE":  storage.Float64Type,
	"FLOAT32": storage.Float32Type,
	// String and character types
	"STRING": storage.StringType,
	"TEXT":   storage.TextType,
	"RUNE":   storage.RuneType,
	"BYTE":   storage.ByteType,
	// Boolean type
	"BOOL":    storage.BoolType,
	"BOOLEAN": storage.BoolType,
	// Time types
	"TIME":      storage.TimeType,
	"DATE":      storage.DateType,
	"DATETIME":  storage.DateTimeType,
	"TIMESTAMP": storage.TimestampType,
	"DURATION":  storage.DurationType,
	// Complex data types
	"JSON":  storage.JsonType,
	"JSONB": storage.JsonbType,
	"MAP":   storage.MapType,
	"SLICE": storage.SliceType,
	"ARRAY": storage.SliceType,
	// Advanced types
	"COMPLEX64":  storage.Complex64Type,
	"COMPLEX128": storage.Complex128Type,
	"COMPLEX":    storage.Complex128Type,
	"POINTER":    storage.PointerType,
	"PTR":        storage.PointerType,
	"INTERFACE":  storage.InterfaceType,
	// Vector types (for RAG / embedding storage)
	"VECTOR":    storage.VectorType,
	"EMBEDDING": storage.VectorType,
	// Spatial types (GeoJSON geometry storage). GEOMETRY(SRID)-style
	// parameters are not supported: a parenthesized argument after the type
	// name is only recognized here when the base name also matches a
	// sqliteAffinity substring rule, which "GEOMETRY" does not, so
	// GEOMETRY(4326) would silently fall through to DECIMAL exactly like the
	// unparameterized keyword did before this entry existed.
	"GEOMETRY": storage.GeometryType,
	"GEOM":     storage.GeometryType,
	// Extra data types
	"YAML":   storage.YAMLType,
	"URL":    storage.URLType,
	"HASH":   storage.HASHType,
	"BITMAP": storage.BitmapType,
	// Existing native types which were previously only usable through the
	// storage API. They also make ordinary SQLite schemas more portable.
	"DECIMAL":  storage.DecimalType,
	"NUMERIC":  storage.DecimalType,
	"MONEY":    storage.MoneyType,
	"UUID":     storage.UUIDType,
	"XML":      storage.XMLType,
	"INTERVAL": storage.IntervalType,
	// Binary data. BLOB is the canonical SQL spelling; the aliases make it
	// practical to import SQLite/PostgreSQL-ish schemas without weakening the
	// runtime invariant that BlobType values are always []byte.
	"BLOB":      storage.BlobType,
	"BYTEA":     storage.BlobType,
	"BINARY":    storage.BlobType,
	"VARBINARY": storage.BlobType,
}

type parsedColumnType struct {
	typ      storage.ColType
	declared string
	affinity storage.SQLiteAffinity
}

// parseColumnType accepts both tinySQL's native names and SQLite's permissive
// type declarations. SQLite's type system is affinity-based, so declarations
// such as VARCHAR(255), DOUBLE PRECISION and UNSIGNED BIG INT must not force
// the engine to invent a separate physical value type.
func (p *Parser) parseColumnType() (parsedColumnType, error) {
	// SQLite allows a column without a declared type. It has BLOB affinity and
	// accepts every storage class, represented here by InterfaceType.
	if p.isColumnTypeTerminator() {
		return parsedColumnType{typ: storage.InterfaceType, affinity: storage.AffinityBlob}, nil
	}
	if p.cur.Typ != tKeyword && p.cur.Typ != tIdent {
		return parsedColumnType{}, p.errf("expected column type")
	}

	words := make([]string, 0, 3)
	var arguments string
	for (p.cur.Typ == tKeyword || p.cur.Typ == tIdent) && !p.isColumnTypeTerminator() {
		words = append(words, upper(p.cur.Val))
		p.next()
		// A SQLite type name may be followed by one parenthesized length or
		// precision list. Its values are schema decoration, not runtime limits.
		if p.cur.Typ == tSymbol && p.cur.Val == "(" {
			var err error
			arguments, err = p.skipTypeArguments()
			if err != nil {
				return parsedColumnType{}, err
			}
			break
		}
	}
	if len(words) == 0 {
		return parsedColumnType{}, p.errf("expected column type")
	}

	declared := strings.Join(words, " ") + arguments
	// ANY is the SQLite STRICT-table escape hatch. It has no coercion and
	// preserves the value's original storage class, so InterfaceType is the
	// existing tinySQL representation rather than a new value type.
	if declared == "ANY" {
		return parsedColumnType{typ: storage.InterfaceType, declared: declared, affinity: storage.AffinityBlob}, nil
	}
	if typ, ok := typeKeywordMap[declared]; ok {
		// These names have SQLite-defined affinity semantics. Keep native
		// tinySQL spellings such as INT8, JSON and TIMESTAMP strict.
		switch declared {
		case "INT", "FLOAT", "DOUBLE", "TEXT", "NUMERIC", "DECIMAL", "BOOL", "BOOLEAN":
			return parsedColumnType{typ: typ, declared: declared, affinity: sqliteAffinity(declared)}, nil
		default:
			return parsedColumnType{typ: typ, declared: declared}, nil
		}
	}
	// A one-token native type retains existing tinySQL coercion semantics.
	if len(words) == 1 && arguments == "" {
		if typ, ok := typeKeywordMap[words[0]]; ok {
			return parsedColumnType{typ: typ, declared: declared}, nil
		}
	}

	affinity := sqliteAffinity(declared)
	typ := storage.InterfaceType
	switch affinity {
	case storage.AffinityInteger:
		typ = storage.IntType
	case storage.AffinityReal:
		typ = storage.FloatType
	case storage.AffinityText:
		typ = storage.TextType
	case storage.AffinityNumeric:
		typ = storage.DecimalType
	case storage.AffinityBlob:
		// A declared BLOB is a binary tinySQL column. A type-less column is
		// represented above as InterfaceType so it remains fully dynamic.
		if declared == "BLOB" {
			typ = storage.BlobType
			affinity = storage.AffinityDefault
		}
	}
	return parsedColumnType{typ: typ, declared: declared, affinity: affinity}, nil
}

// nonKeywordConstraintWords are constraint-introducing words that are
// deliberately absent from the lexer's keyword allow-list, so they arrive as
// tIdent tokens carrying their original spelling. They are matched by name
// here instead of being promoted to keywords because a keyword is global:
// "CHECK", "COLLATE" and "CONSTRAINT" are all plausible column names, and
// making them keywords would change how they lex in every expression in every
// statement to fix a problem that only exists inside a column definition.
var nonKeywordConstraintWords = map[string]bool{
	"CHECK":         true,
	"COLLATE":       true,
	"GENERATED":     true,
	"AUTOINCREMENT": true,
	"CONSTRAINT":    true,
}

// constraintWord returns the current token as an upper-case constraint word,
// or "" when the token cannot introduce a constraint at all. Keywords keep
// their already-uppercased value; the words above are recognised in their
// tIdent form. Callers must still decide which words are meaningful in their
// position — this only normalises the spelling.
func (p *Parser) constraintWord() string {
	switch p.cur.Typ {
	case tKeyword:
		return p.cur.Val
	case tIdent:
		if w := upper(p.cur.Val); nonKeywordConstraintWords[w] {
			return w
		}
	}
	return ""
}

// isColumnTypeTerminator reports whether the current token ends the declared
// type of a column definition.
//
// CHECK, GENERATED, AS, COLLATE, AUTOINCREMENT and CONSTRAINT were missing
// here, and their absence was not a cosmetic gap: parseColumnType's loop
// swallowed them (plus any parenthesised argument) into the declared type
// string, so "b INT CHECK (b > 0)" produced a column of declared type
// "INT CHECK(b>0)" whose predicate nothing ever enforced -- an unenforced
// CHECK accepts exactly the rows it exists to reject. Terminating the type
// hands these clauses to parseColumnConstraints, which rejects the ones
// tinySQL cannot honour by name instead of pretending to support them.
func (p *Parser) isColumnTypeTerminator() bool {
	if p.cur.Typ == tSymbol && (p.cur.Val == "," || p.cur.Val == ")") {
		return true
	}
	switch p.constraintWord() {
	case "PRIMARY", "FOREIGN", "UNIQUE", "REFERENCES", "NOT", "NULL", "DEFAULT",
		"CHECK", "GENERATED", "AS", "COLLATE", "AUTOINCREMENT", "CONSTRAINT":
		return true
	default:
		return false
	}
}

func (p *Parser) skipTypeArguments() (string, error) {
	depth := 0
	var b strings.Builder
	for p.cur.Typ == tSymbol && p.cur.Val == "(" || depth > 0 {
		if p.cur.Typ == tEOF {
			return "", p.errf("unterminated type arguments")
		}
		b.WriteString(p.cur.Val)
		if p.cur.Typ == tSymbol {
			switch p.cur.Val {
			case "(":
				depth++
			case ")":
				depth--
			}
		}
		p.next()
		if depth == 0 {
			return b.String(), nil
		}
	}
	return "", nil
}

func sqliteAffinity(declared string) storage.SQLiteAffinity {
	// This is SQLite's documented affinity algorithm, applied to the upper
	// case declaration. Order matters: "FLOATING POINT" contains "INT".
	base := declared
	if i := strings.IndexByte(base, '('); i >= 0 {
		base = base[:i]
	}
	switch {
	case strings.Contains(base, "INT"):
		return storage.AffinityInteger
	case strings.Contains(base, "CHAR"), strings.Contains(base, "CLOB"), strings.Contains(base, "TEXT"):
		return storage.AffinityText
	case base == "", strings.Contains(base, "BLOB"):
		return storage.AffinityBlob
	case strings.Contains(base, "REAL"), strings.Contains(base, "FLOA"), strings.Contains(base, "DOUB"):
		return storage.AffinityReal
	default:
		return storage.AffinityNumeric
	}
}

func (p *Parser) parseIdentLike() string {
	// Accept both identifiers and keywords as identifier-like names.
	// This allows column/table names like "timestamp" even if they are keywords.
	if p.cur.Typ == tIdent || p.cur.Typ == tKeyword {
		s := p.cur.Val
		p.next()
		return s
	}
	return ""
}

func (p *Parser) parseQualifiedIdentLike() string {
	name := p.parseIdentLike()
	if name == "" {
		return ""
	}
	parts := []string{name}
	for p.cur.Typ == tSymbol && p.cur.Val == "." {
		p.next()
		part := p.parseIdentLike()
		if part == "" {
			return strings.Join(parts, ".")
		}
		parts = append(parts, part)
	}
	return strings.Join(parts, ".")
}
