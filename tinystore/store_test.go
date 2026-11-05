package tinystore

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"

	tsql "github.com/SimonWaldherr/tinySQL"
)

// All names and numbers are invented.

func table(name string, cols []tsql.Column, rows [][]any) *tsql.Table {
	t := tsql.NewTable(name, cols, false)
	t.Rows = rows
	return t
}

// sameTable compares two tables value by value, including the Go type of every value (an INT
// column must come back as int, not int64) and the bit pattern of every float (NaN and -0).
func sameTable(t *testing.T, want, got *tsql.Table) {
	t.Helper()
	if want.Name != got.Name || len(want.Cols) != len(got.Cols) || len(want.Rows) != len(got.Rows) {
		t.Fatalf("shape: %q %d cols %d rows, got %q %d cols %d rows", want.Name, len(want.Cols), len(want.Rows), got.Name, len(got.Cols), len(got.Rows))
	}
	for i, c := range want.Cols {
		if c.Name != got.Cols[i].Name || c.Type != got.Cols[i].Type {
			t.Fatalf("column %d: %v, got %v", i, c, got.Cols[i])
		}
	}
	for r, row := range want.Rows {
		if len(row) != len(got.Rows[r]) {
			t.Fatalf("row %d has %d values, got %d", r, len(row), len(got.Rows[r]))
		}
		for c, v := range row {
			g := got.Rows[r][c]
			switch x := v.(type) {
			case float64:
				y, ok := g.(float64)
				if !ok || math.Float64bits(x) != math.Float64bits(y) {
					t.Fatalf("row %d column %d: %v (%T), got %v (%T)", r, c, v, v, g, g)
				}
			case int64:
				// stored as int64 or int, read back as int (what tinySQL's INT column holds)
				y, ok := g.(int)
				if !ok || int64(y) != x {
					t.Fatalf("row %d column %d: %v, got %v (%T)", r, c, v, g, g)
				}
			default:
				if v != g {
					t.Fatalf("row %d column %d: %v (%T), got %v (%T)", r, c, v, v, g, g)
				}
			}
		}
	}
}

func writeRead(t *testing.T, tbl *tsql.Table) *tsql.Table {
	t.Helper()
	dir := t.TempDir()
	if _, err := Write(dir, tbl); err != nil {
		t.Fatal(err)
	}
	got, err := Read(filepath.Join(dir, FileName(tbl.Name)))
	if err != nil {
		t.Fatal(err)
	}
	return got
}

func TestRoundTripAllTypesAndEdgeValues(t *testing.T) {
	maxInt := int(^uint(0) >> 1)
	nan := math.Float64frombits(0x7ff8000000000123)
	negZero := math.Copysign(0, -1)
	tbl := table("Sales", []tsql.Column{
		{Name: "region", Type: tsql.TextType},
		{Name: "n", Type: tsql.IntType},
		{Name: "revenue", Type: tsql.FloatType},
		{Name: "note", Type: tsql.TextType},
	}, [][]any{
		{"Nord", 1, 1.5, "ä ö ü ß – 日本語 🙂"},
		{"Süd", 0, 0.0, ""},
		{"Nord", -1, negZero, nil},
		{nil, maxInt, math.MaxFloat64, "line\nbreak\x00nul"},
		{"", -maxInt - 1, math.SmallestNonzeroFloat64, "x"},
		{"Nord", int64(42), math.Inf(1), "x"},
		{"Ost", nil, math.Inf(-1), "x"},
		{"Ost", 7, nan, "x"},
		{"West", 300, nil, "x"},
	})
	got := writeRead(t, tbl)
	// int64 inputs are accepted and come back as int
	tbl.Rows[5][1] = int64(42)
	sameTable(t, tbl, got)
	if _, ok := got.Rows[0][1].(int); !ok {
		t.Fatalf("an INT column must come back as int, got %T", got.Rows[0][1])
	}
}

func TestRoundTripEmptyAndTinyTables(t *testing.T) {
	cols := []tsql.Column{{Name: "a", Type: tsql.TextType}, {Name: "b", Type: tsql.IntType}, {Name: "c", Type: tsql.FloatType}}
	for _, rows := range [][][]any{
		{},
		{{nil, nil, nil}},
		{{"x", 1, 2.0}},
		{{nil, nil, nil}, {nil, nil, nil}},
	} {
		sameTable(t, table("t", cols, rows), writeRead(t, table("t", cols, rows)))
	}
}

func TestTextPlainWhenTooManyDistinctValues(t *testing.T) {
	n := DictMaxDistinct + 5000 // some rows are NULL or empty and add no distinct value
	rows := make([][]any, n)
	for i := range rows {
		switch {
		case i%97 == 0:
			rows[i] = []any{nil}
		case i%89 == 0:
			rows[i] = []any{""}
		default:
			rows[i] = []any{fmt.Sprintf("Beleg %d", i)}
		}
	}
	tbl := table("t", []tsql.Column{{Name: "no", Type: tsql.TextType}}, rows)
	dir := t.TempDir()
	if _, err := Write(dir, tbl); err != nil {
		t.Fatal(err)
	}
	f, _ := os.Open(filepath.Join(dir, FileName("t")))
	defer f.Close()
	fi, _ := f.Stat()
	h, err := readHeader(f, fi.Size())
	if err != nil {
		t.Fatal(err)
	}
	if h.cols[0].enc != encTextPlain {
		t.Fatalf("more than %d distinct values must be stored as plain text, got encoding %d", DictMaxDistinct, h.cols[0].enc)
	}
	got, err := Read(filepath.Join(dir, FileName("t")))
	if err != nil {
		t.Fatal(err)
	}
	sameTable(t, tbl, got)
}

func TestDictionaryReferencesCrossVarintBoundary(t *testing.T) {
	rows := make([][]any, 0, 132)
	for i := 0; i < 130; i++ {
		rows = append(rows, []any{fmt.Sprintf("value-%d", i)})
	}
	rows = append(rows, []any{nil}, []any{"value-129"})
	tbl := table("varints", []tsql.Column{{Name: "text", Type: tsql.TextType}}, rows)
	sameTable(t, tbl, writeRead(t, tbl))
	block, enc, err := encodeText(rows, 0)
	if err != nil || enc != encTextDict {
		t.Fatalf("encoding %d: %v", enc, err)
	}
	// References 127 and 128 use one and two bytes respectively; NULL is zero.
	want := []byte{0x7f, 0x80, 0x01, 0x81, 0x01, 0x82, 0x01, 0x00, 0x82, 0x01}
	if !bytes.HasSuffix(block, want) {
		t.Fatalf("unexpected dictionary references: %x", block[len(block)-len(want):])
	}
}

func TestDictionaryColumnsShareTheirStrings(t *testing.T) {
	rows := make([][]any, 1000)
	for i := range rows {
		rows[i] = []any{strings.Clone([]string{"Nord", "Süd", "Ost"}[i%3])}
	}
	got := writeRead(t, table("t", []tsql.Column{{Name: "r", Type: tsql.TextType}}, rows))
	seen := map[any]bool{}
	for _, row := range got.Rows {
		seen[row[0]] = true
	}
	if len(seen) != 3 {
		t.Fatalf("3 values must stay 3 values: %v", seen)
	}
}

func TestRandomTablesRoundTrip(t *testing.T) {
	rng := rand.New(rand.NewSource(7))
	for iter := 0; iter < 60; iter++ {
		ncols := 1 + rng.Intn(14)
		nrows := rng.Intn(3000)
		cols := make([]tsql.Column, ncols)
		card := make([]int, ncols)
		nullRate := make([]float64, ncols)
		for c := range cols {
			cols[c] = tsql.Column{Name: fmt.Sprintf("c%d", c), Type: []tsql.ColType{tsql.TextType, tsql.IntType, tsql.FloatType}[rng.Intn(3)]}
			card[c] = 1 + rng.Intn(500)
			nullRate[c] = []float64{0, 0, 0.01, 0.5, 1}[rng.Intn(5)]
		}
		rows := make([][]any, nrows)
		for r := range rows {
			row := make([]any, ncols)
			for c := range row {
				if rng.Float64() < nullRate[c] {
					continue
				}
				k := rng.Intn(card[c])
				switch cols[c].Type {
				case tsql.TextType:
					row[c] = fmt.Sprintf("v%d-%s", k, strings.Repeat("é", k%4))
				case tsql.IntType:
					row[c] = k*7919 - 1000
					if rng.Intn(50) == 0 {
						row[c] = rng.Int()
					}
				default:
					row[c] = float64(k) / 7
				}
			}
			rows[r] = row
		}
		tbl := table(fmt.Sprintf("T%d", iter), cols, rows)
		sameTable(t, tbl, writeRead(t, tbl))
	}
}

func TestWriteRefusesWhatItCannotStoreExactly(t *testing.T) {
	good := []tsql.Column{{Name: "a", Type: tsql.TextType}}
	for name, tbl := range map[string]*tsql.Table{
		"bool column":       table("t", []tsql.Column{{Name: "a", Type: tsql.BoolType}}, [][]any{{true}}),
		"number in text":    table("t", good, [][]any{{1}}),
		"text in int":       table("t", []tsql.Column{{Name: "a", Type: tsql.IntType}}, [][]any{{"1"}}),
		"int in float":      table("t", []tsql.Column{{Name: "a", Type: tsql.FloatType}}, [][]any{{1}}),
		"ragged row":        table("t", []tsql.Column{{Name: "a", Type: tsql.TextType}, {Name: "b", Type: tsql.TextType}}, [][]any{{"x"}}),
		"no columns":        table("t", nil, nil),
		"float in int":      table("t", []tsql.Column{{Name: "a", Type: tsql.IntType}}, [][]any{{1.5}}),
		"bytes in text":     table("t", good, [][]any{{[]byte("x")}}),
		"too many columns":  table("t", make([]tsql.Column, maxCols+1), nil),
		"unsupported plain": table("t", good, [][]any{{"ok"}, {2.5}}),
	} {
		dir := t.TempDir()
		if _, err := Write(dir, tbl); err == nil {
			t.Errorf("%s: must be refused", name)
		}
		if m, _ := filepath.Glob(filepath.Join(dir, "*")); len(m) != 0 {
			t.Errorf("%s: a refused table must leave no file behind, found %v", name, m)
		}
	}
}

func TestWriteIsAtomicAndReplaces(t *testing.T) {
	dir := t.TempDir()
	cols := []tsql.Column{{Name: "a", Type: tsql.IntType}}
	if _, err := Write(dir, table("t", cols, [][]any{{1}})); err != nil {
		t.Fatal(err)
	}
	if _, err := Write(dir, table("T", cols, [][]any{{1}, {2}})); err != nil { // same table, other case
		t.Fatal(err)
	}
	m, _ := filepath.Glob(filepath.Join(dir, "*"))
	if len(m) != 1 || strings.HasSuffix(m[0], ".tmp") {
		t.Fatalf("one file, no temporary leftovers: %v", m)
	}
	got, err := Read(m[0])
	if err != nil || len(got.Rows) != 2 {
		t.Fatalf("the later write replaces the earlier: %v %v", err, got)
	}
	if !Has(dir) || Has(t.TempDir()) {
		t.Fatal("Has must see exactly the directories with a table file")
	}
}

func TestFileNameCannotEscapeTheDirectory(t *testing.T) {
	for _, name := range []string{"../x", `a\b`, "con", "a/b", "ä", "", strings.Repeat("x", 100)} {
		fn := FileName(name)
		if filepath.Base(fn) != fn || strings.ContainsAny(fn, `/\:`) || !strings.HasSuffix(fn, Ext) {
			t.Errorf("FileName(%q) = %q", name, fn)
		}
	}
	if FileName("Sales") != FileName("sales") {
		t.Error("names that differ only in case share a table, so they share a file")
	}
}

func sampleFile(t *testing.T) (string, []byte) {
	t.Helper()
	rows := make([][]any, 40)
	for i := range rows {
		rows[i] = []any{[]string{"Nord", "Süd"}[i%2], i * 3, float64(i) / 4, nil}
		if i%5 == 0 {
			rows[i][1], rows[i][2] = nil, nil
		}
		if i%7 == 0 {
			rows[i][3] = "x"
		}
	}
	tbl := table("t", []tsql.Column{{Name: "r", Type: tsql.TextType}, {Name: "n", Type: tsql.IntType}, {Name: "v", Type: tsql.FloatType}, {Name: "s", Type: tsql.TextType}}, rows)
	dir := t.TempDir()
	if _, err := Write(dir, tbl); err != nil {
		t.Fatal(err)
	}
	p := filepath.Join(dir, FileName("t"))
	b, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	return p, b
}

// Every byte of the file is covered by the magic, a checksum or a structural check, so changing
// any single byte must be reported, never read as a different table and never panic.
func TestEverySingleByteChangeIsDetected(t *testing.T) {
	p, orig := sampleFile(t)
	for i := range orig {
		b := append([]byte(nil), orig...)
		b[i] ^= 0xff
		if err := os.WriteFile(p, b, 0o644); err != nil {
			t.Fatal(err)
		}
		if _, err := Read(p); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("byte %d of %d changed: %v", i, len(orig), err)
		}
		b[i] = orig[i] ^ 0x01 // and a single bit
		_ = os.WriteFile(p, b, 0o644)
		if _, err := Read(p); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("bit of byte %d changed: %v", i, err)
		}
	}
}

func TestEveryTruncationAndAppendIsDetected(t *testing.T) {
	p, orig := sampleFile(t)
	for n := 0; n < len(orig); n++ {
		if err := os.WriteFile(p, orig[:n], 0o644); err != nil {
			t.Fatal(err)
		}
		if _, err := Read(p); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("file cut to %d of %d bytes: %v", n, len(orig), err)
		}
	}
	if err := os.WriteFile(p, append(append([]byte(nil), orig...), 'x'), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Read(p); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("appended byte: %v", err)
	}
	if err := os.WriteFile(p, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Read(p); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("empty file: %v", err)
	}
}

func TestReadDirIsStrictAndSorted(t *testing.T) {
	dir := t.TempDir()
	cols := []tsql.Column{{Name: "a", Type: tsql.IntType}}
	for _, n := range []string{"b", "a", "c"} {
		if _, err := Write(dir, table(n, cols, [][]any{{1}})); err != nil {
			t.Fatal(err)
		}
	}
	got, err := ReadDir(dir)
	if err != nil || len(got) != 3 {
		t.Fatalf("%v %d", err, len(got))
	}
	// sorted by file name, which is the hex of the lower-case table name: a < b < c
	if got[0].Name != "a" || got[1].Name != "b" || got[2].Name != "c" {
		t.Fatalf("order: %s %s %s", got[0].Name, got[1].Name, got[2].Name)
	}
	if err := os.WriteFile(filepath.Join(dir, FileName("b")), []byte("kaputt"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadDir(dir); !errors.Is(err, ErrCorrupt) || !strings.Contains(err.Error(), FileName("b")) {
		t.Fatalf("one damaged file fails the whole directory and names the file: %v", err)
	}
}

// A footer that passes its checksum but lies must still be refused (a bug in a writer, not
// damage on disk): the counts and offsets have to describe the blocks exactly.
func TestForgedFooterIsRefused(t *testing.T) {
	p, orig := sampleFile(t)
	flen := int(binary.LittleEndian.Uint32(orig[len(orig)-trailerLen:]))
	fstart := len(orig) - trailerLen - crcLen - flen
	f, err := os.Open(p)
	if err != nil {
		t.Fatal(err)
	}
	h, err := readHeader(f, int64(len(orig)))
	f.Close()
	if err != nil {
		t.Fatal(err)
	}
	forge := func(nrows int, metas []colMeta) []byte {
		footer := appendFooter(nil, h.name, nrows, metas)
		b := append([]byte(nil), orig[:fstart]...)
		b = append(b, footer...)
		b = binary.LittleEndian.AppendUint32(b, crc32.Checksum(footer, crcTable))
		b = binary.LittleEndian.AppendUint32(b, uint32(len(footer)))
		return append(b, magic...)
	}
	clone := func() []colMeta { return append([]colMeta(nil), h.cols...) }
	cases := map[string][]byte{}
	cases["more rows than any block can hold"] = forge(1<<30, clone())
	cases["one row too many"] = forge(h.nrows+1, clone())
	cases["one row too few"] = forge(h.nrows-1, clone())
	m := clone()
	m[1].offset++
	cases["shifted block"] = forge(h.nrows, m)
	m = clone()
	m[0].length--
	cases["shortened block"] = forge(h.nrows, m)
	m = clone()
	m[0].enc = encFloat
	cases["encoding that does not fit the type"] = forge(h.nrows, m)
	m = clone()
	m[2].typ = typeInt
	cases["type that does not fit the encoding"] = forge(h.nrows, m)
	cases["a column missing"] = forge(h.nrows, clone()[:len(h.cols)-1])
	m = clone()
	m[0].typ = 9
	cases["unknown type"] = forge(h.nrows, m)
	if len(cases) < 9 {
		t.Fatal("cases lost")
	}
	// the unmodified forgery path must work, or the cases above prove nothing
	if err := os.WriteFile(p, forge(h.nrows, clone()), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Read(p); err != nil {
		t.Fatalf("a footer written the normal way must read: %v", err)
	}
	for name, b := range cases {
		if err := os.WriteFile(p, b, 0o644); err != nil {
			t.Fatal(err)
		}
		if _, err := Read(p); !errors.Is(err, ErrCorrupt) {
			t.Errorf("%s: %v", name, err)
		}
	}
}

// FuzzRead feeds arbitrary bytes to the reader: whatever a file contains, reading it returns a
// table or an error, never a panic and never an allocation out of proportion to the file.
func FuzzRead(f *testing.F) {
	_, orig := func() (string, []byte) {
		rows := [][]any{{"a", 1, 1.5}, {nil, nil, nil}, {"b", -7, 0.0}}
		tbl := table("t", []tsql.Column{{Name: "r", Type: tsql.TextType}, {Name: "n", Type: tsql.IntType}, {Name: "v", Type: tsql.FloatType}}, rows)
		dir := f.TempDir()
		if _, err := Write(dir, tbl); err != nil {
			f.Fatal(err)
		}
		b, _ := os.ReadFile(filepath.Join(dir, FileName("t")))
		return "", b
	}()
	f.Add(orig)
	f.Add([]byte(magic))
	f.Add([]byte{})
	f.Add(append([]byte(magic), make([]byte, 64)...))
	dir := f.TempDir()
	p := filepath.Join(dir, "fuzz"+Ext)
	f.Fuzz(func(t *testing.T, data []byte) {
		if err := os.WriteFile(p, data, 0o644); err != nil {
			t.Fatal(err)
		}
		tbl, err := Read(p)
		if err != nil {
			return
		}
		// What reads must also write and read back the same.
		out := t.TempDir()
		if _, err := Write(out, tbl); err != nil {
			t.Fatalf("a table that was read must be writable: %v", err)
		}
		again, err := Read(filepath.Join(out, FileName(tbl.Name)))
		if err != nil {
			t.Fatal(err)
		}
		if len(again.Rows) != len(tbl.Rows) {
			t.Fatalf("row count changed: %d vs %d", len(again.Rows), len(tbl.Rows))
		}
	})
}

// A data folder whose path contains glob metacharacters must still be found: Has and ReadDir
// list the folder, they do not use it as a pattern.
func TestHasAndReadDirWithMetacharactersInPath(t *testing.T) {
	base := t.TempDir()
	dir := filepath.Join(base, "Daten [Backup] [x", "copy")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if Has(dir) {
		t.Fatal("empty folder reported as holding tables")
	}
	want := table("t1", []tsql.Column{{Name: "a", Type: tsql.IntType}}, [][]any{{1}, {2}})
	if _, err := Write(dir, want); err != nil {
		t.Fatal(err)
	}
	if !Has(dir) {
		t.Fatal("Has did not find the table in a folder with metacharacters in its path")
	}
	got, err := ReadDir(dir)
	if err != nil || len(got) != 1 {
		t.Fatalf("ReadDir: %d tables, err %v", len(got), err)
	}
	sameTable(t, want, got[0])
	if Has(filepath.Join(base, "missing")) {
		t.Fatal("missing folder reported as holding tables")
	}
	if g, err := ReadDir(filepath.Join(base, "missing")); err != nil || len(g) != 0 {
		t.Fatalf("missing folder: %d tables, err %v", len(g), err)
	}
}
