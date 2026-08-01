package engine

// BLOB column storage through the documented constructors.
//
// The BLOB_* constructors used to return a lowercase hex string, so
//
//	INSERT INTO t (data) VALUES (BLOB_FROM_HEX('89504e47'))
//
// failed with "cannot convert string to BLOB" — the documented way to build
// binary data could not be stored in the column type meant to hold it. Rejecting
// plain text for a BLOB column is deliberate (internal/driver/blob_test.go), and
// a BLOB value is a []byte everywhere else in the engine: X'hex' literals parse
// to []byte, coerceToBlob accepts []byte, the driver binds and scans []byte. So
// the constructors were the side that had to change.
//
// These tests pin that end to end, and pin what did *not* change: text literals
// are still refused, and the rendering functions still return text.

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func blobColumnDB(t *testing.T) (*storage.DB, context.Context) {
	t.Helper()
	db := storage.NewDB()
	ctx := context.Background()
	if _, err := Execute(ctx, db, "default", mustParse(
		`CREATE TABLE files (id INT, data BLOB, label TEXT)`)); err != nil {
		t.Fatal(err)
	}
	return db, ctx
}

// TestInsertBlobFromHexIntoBlobColumn is the case the fix exists for.
func TestInsertBlobFromHexIntoBlobColumn(t *testing.T) {
	db, ctx := blobColumnDB(t)
	// A PNG signature: binary that is not valid UTF-8, so it could not travel as
	// a text literal.
	want := []byte{0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a}

	if _, err := Execute(ctx, db, "default", mustParse(fmt.Sprintf(
		`INSERT INTO files VALUES (1, BLOB_FROM_HEX('%s'), 'png')`, hex.EncodeToString(want)))); err != nil {
		t.Fatalf("INSERT ... BLOB_FROM_HEX: %v", err)
	}

	// Stored as raw bytes, not as hex text.
	table, err := db.Get("default", "files")
	if err != nil {
		t.Fatal(err)
	}
	dataIdx, err := table.ColIndex("data")
	if err != nil {
		t.Fatal(err)
	}
	stored, ok := table.Rows[0][dataIdx].([]byte)
	if !ok {
		t.Fatalf("stored value is %T, want []byte", table.Rows[0][dataIdx])
	}
	if !bytes.Equal(stored, want) {
		t.Errorf("stored %x, want %x", stored, want)
	}

	// BLOB_LENGTH and BLOB_HEX read it back correctly.
	rs, err := Execute(ctx, db, "default", mustParse(
		`SELECT BLOB_LENGTH(data) AS n, BLOB_HEX(data) AS h FROM files WHERE id = 1`))
	if err != nil {
		t.Fatal(err)
	}
	n, _ := ragValue(rs.Rows[0], "n")
	if got, _ := toInt(n); got != len(want) {
		t.Errorf("BLOB_LENGTH = %v, want %d", n, len(want))
	}
	h, _ := ragValue(rs.Rows[0], "h")
	if h != hex.EncodeToString(want) {
		t.Errorf("BLOB_HEX = %v, want %s", h, hex.EncodeToString(want))
	}

	// And the column itself scans back as bytes.
	rs, err = Execute(ctx, db, "default", mustParse(`SELECT data FROM files WHERE id = 1`))
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := ragValue(rs.Rows[0], "data")
	got, ok := raw.([]byte)
	if !ok {
		t.Fatalf("SELECT returned %T for a BLOB column, want []byte", raw)
	}
	if !bytes.Equal(got, want) {
		t.Errorf("round trip changed the payload: %x != %x", got, want)
	}
}

// TestInsertBlobFromBase64IntoBlobColumn covers the other documented constructor.
func TestInsertBlobFromBase64IntoBlobColumn(t *testing.T) {
	db, ctx := blobColumnDB(t)
	want := []byte{0x00, 0xff, 0x10, 0x7f, 0x80}

	if _, err := Execute(ctx, db, "default", mustParse(fmt.Sprintf(
		`INSERT INTO files VALUES (1, BLOB_FROM_BASE64('%s'), 'bin')`,
		base64.StdEncoding.EncodeToString(want)))); err != nil {
		t.Fatalf("INSERT ... BLOB_FROM_BASE64: %v", err)
	}
	rs, err := Execute(ctx, db, "default", mustParse(`SELECT data FROM files WHERE id = 1`))
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := ragValue(rs.Rows[0], "data")
	got, ok := raw.([]byte)
	if !ok {
		t.Fatalf("got %T, want []byte", raw)
	}
	if !bytes.Equal(got, want) {
		t.Errorf("stored %x, want %x", got, want)
	}
}

// TestInsertBlobSubstrAndConcatIntoBlobColumn checks the manipulating functions
// are storable too, so a blob can be built in SQL rather than only in Go.
func TestInsertBlobSubstrAndConcatIntoBlobColumn(t *testing.T) {
	db, ctx := blobColumnDB(t)

	stmts := []string{
		`INSERT INTO files VALUES (1, BLOB_CONCAT(X'89504e47', X'0d0a1a0a'), 'concat')`,
		`INSERT INTO files VALUES (2, BLOB_SUBSTR(X'89504e470d0a1a0a', 0, 4), 'substr')`,
		// Nested: build from hex, slice it, store the slice.
		`INSERT INTO files VALUES (3, BLOB_SUBSTR(BLOB_FROM_HEX('deadbeefcafe'), 2, 2), 'nested')`,
	}
	for _, sql := range stmts {
		if _, err := Execute(ctx, db, "default", mustParse(sql)); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
	}

	rs, err := Execute(ctx, db, "default", mustParse(
		`SELECT id, BLOB_HEX(data) AS h FROM files ORDER BY id`))
	if err != nil {
		t.Fatal(err)
	}
	want := map[int]string{
		1: "89504e470d0a1a0a",
		2: "89504e47",
		3: "beef",
	}
	if len(rs.Rows) != len(want) {
		t.Fatalf("got %d rows, want %d", len(rs.Rows), len(want))
	}
	for _, row := range rs.Rows {
		idVal, _ := ragValue(row, "id")
		id, _ := toInt(idVal)
		h, _ := ragValue(row, "h")
		if h != want[id] {
			t.Errorf("id %d: BLOB_HEX = %v, want %s", id, h, want[id])
		}
	}
}

// TestBlobColumnStillRejectsText pins the deliberate behaviour the fix must not
// loosen: a plain text literal is not a blob. Were coerceToBlob relaxed to accept
// strings instead, this would silently store text — and a string that happened to
// be valid hex would be ambiguous between four bytes and eight characters.
func TestBlobColumnStillRejectsText(t *testing.T) {
	db, ctx := blobColumnDB(t)
	for _, sql := range []string{
		`INSERT INTO files VALUES (1, 'text is not a blob', 'bad')`,
		// Valid hex as text is still text, and must not be silently decoded.
		`INSERT INTO files VALUES (2, 'deadbeef', 'bad')`,
	} {
		if _, err := Execute(ctx, db, "default", mustParse(sql)); err == nil {
			t.Errorf("%s: expected a type error, got none", sql)
		}
	}
}

// TestBlobFunctionReturnTypes pins the split: functions returning a blob return
// []byte, functions rendering one return a string. Getting this backwards is what
// caused the original problem, so it is asserted directly.
func TestBlobFunctionReturnTypes(t *testing.T) {
	db := storage.NewDB()
	ctx := context.Background()

	blobReturning := []string{
		`BLOB_FROM_HEX('deadbeef')`,
		`BLOB_FROM_BASE64('3q2+7w==')`,
		`BLOB_SUBSTR(X'deadbeef', 1, 2)`,
		`BLOB_CONCAT(X'dead', X'beef')`,
	}
	for _, expr := range blobReturning {
		rs, err := Execute(ctx, db, "default", mustParse(`SELECT `+expr+` AS v`))
		if err != nil {
			t.Fatalf("%s: %v", expr, err)
		}
		v, _ := ragValue(rs.Rows[0], "v")
		if _, ok := v.([]byte); !ok {
			t.Errorf("%s returned %T, want []byte so it can be stored in a BLOB column", expr, v)
		}
	}

	textReturning := []string{
		`BLOB_HEX(X'deadbeef')`,
		`BLOB_TO_BASE64(X'deadbeef')`,
	}
	for _, expr := range textReturning {
		rs, err := Execute(ctx, db, "default", mustParse(`SELECT `+expr+` AS v`))
		if err != nil {
			t.Fatalf("%s: %v", expr, err)
		}
		v, _ := ragValue(rs.Rows[0], "v")
		if _, ok := v.(string); !ok {
			t.Errorf("%s returned %T, want string — it renders a blob as text", expr, v)
		}
	}

	// BLOB_LENGTH is numeric and BLOB_EQUAL boolean, unchanged.
	rs, err := Execute(ctx, db, "default", mustParse(
		`SELECT BLOB_LENGTH(X'deadbeef') AS n, BLOB_EQUAL(X'dead', BLOB_FROM_HEX('dead')) AS eq`))
	if err != nil {
		t.Fatal(err)
	}
	n, _ := ragValue(rs.Rows[0], "n")
	if got, _ := toInt(n); got != 4 {
		t.Errorf("BLOB_LENGTH = %v, want 4", n)
	}
	eq, _ := ragValue(rs.Rows[0], "eq")
	if eq != true {
		t.Errorf("BLOB_EQUAL(X'dead', BLOB_FROM_HEX('dead')) = %v, want true; the two "+
			"representations must compare equal", eq)
	}
}

// TestBlobFunctionsAcceptHexStringsStill checks the compatibility the change
// preserves: blobDecode still takes hex text, so SQL that passed hex strings to
// these functions keeps working.
func TestBlobFunctionsAcceptHexStringsStill(t *testing.T) {
	db := storage.NewDB()
	ctx := context.Background()
	rs, err := Execute(ctx, db, "default", mustParse(
		`SELECT BLOB_LENGTH('deadbeef') AS n, BLOB_HEX('deadbeef') AS h`))
	if err != nil {
		t.Fatal(err)
	}
	n, _ := ragValue(rs.Rows[0], "n")
	if got, _ := toInt(n); got != 4 {
		t.Errorf("BLOB_LENGTH('deadbeef') = %v, want 4 (hex text decodes to 4 bytes)", n)
	}
	h, _ := ragValue(rs.Rows[0], "h")
	if h != "deadbeef" {
		t.Errorf("BLOB_HEX('deadbeef') = %v, want 'deadbeef'", h)
	}
}

// TestBlobSubstrDoesNotAliasSource checks the returned slice owns its bytes.
// A sub-slice of the source would keep the whole blob reachable and let a later
// mutation of one value show up in another.
func TestBlobSubstrDoesNotAliasSource(t *testing.T) {
	db, ctx := blobColumnDB(t)
	if _, err := Execute(ctx, db, "default", mustParse(
		`INSERT INTO files VALUES (1, X'0011223344556677', 'src')`)); err != nil {
		t.Fatal(err)
	}
	rs, err := Execute(ctx, db, "default", mustParse(
		`SELECT BLOB_SUBSTR(data, 2, 3) AS part, data FROM files WHERE id = 1`))
	if err != nil {
		t.Fatal(err)
	}
	partRaw, _ := ragValue(rs.Rows[0], "part")
	part, ok := partRaw.([]byte)
	if !ok {
		t.Fatalf("part is %T, want []byte", partRaw)
	}
	if !bytes.Equal(part, []byte{0x22, 0x33, 0x44}) {
		t.Fatalf("part = %x, want 223344", part)
	}
	srcRaw, _ := ragValue(rs.Rows[0], "data")
	src, _ := srcRaw.([]byte)
	// Mutating the slice must not disturb the source blob.
	part[0] = 0xff
	if src[2] != 0x22 {
		t.Errorf("BLOB_SUBSTR aliases its source: mutating the result changed the stored blob to %x", src)
	}
}
