// Package tinystore stores immutable analytical table snapshots in a compact,
// column-oriented format.
// The TINY01 magic and .tsnap extension identify version 1 of the format.
//
// Snapshots contain table/column names, TEXT/INT/FLOAT types and values only.
// Constraints, defaults, declared types, indexes and statistics are not persisted.
// This is a data interchange format, not a replacement for a database backup or
// tinySQL's transactional storage. Callers must not mutate a table during Write.
//
// Text dictionaries share repeated strings on load. Columns are encoded and
// decoded with bounded parallelism; CRC32C detects accidental corruption.
//
// File layout (little endian, all counts and lengths are unsigned varints unless said):
//
//	"TINY01"                      magic, includes the format version
//	block 0, crc32c(block 0)      one block per column, in column order; the checksum is 4 bytes
//	block 1, crc32c(block 1)
//	...
//	footer, crc32c(footer)        table name, column count, row count, and for every column its
//	                              name, type, encoding, offset and length of its block
//	footer length (uint32)
//	"TINY01"                      magic again: a file that does not end with it was cut off
//
// The footer sits at the end so that a table can be written in one streaming pass without
// knowing the size of the blocks beforehand, and so that a complete file always ends in the
// magic. Offsets in the footer are checked to be exactly consecutive, which makes every
// inconsistency (a shifted block, a cut file, appended garbage) a detected error and not a
// misread table.
//
// Values: a TEXT column holds string or NULL, an INT column int (or int64) or NULL, a FLOAT
// column float64 or NULL. That is all the copy ever stores; any other value or column type
// is refused when writing instead of being stored wrongly.
package tinystore

import (
	"bufio"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"unsafe"

	tsql "github.com/SimonWaldherr/tinySQL"
)

// Ext is the file extension of a table file.
const Ext = ".tsnap"

const (
	magic = "TINY01"
	// maxCols bounds the width of a table. A real copy has a few dozen columns; the bound only
	// keeps a damaged footer from asking for an absurd allocation.
	maxCols = 4096
	// maxRows bounds row counts; readHeader also checks the cell count against
	// the host platform's int range.
	maxRows uint64 = 1 << 31
	// DictMaxDistinct is how many different values a text column may have to be stored as a
	// dictionary. Beyond it (order numbers, free text) the dictionary would be as large as the
	// column and cost a map entry per value, so the column is stored as plain text instead.
	DictMaxDistinct = 100_000
	// crcLen is the size of the checksum after a block and after the footer.
	crcLen = 4
	// trailerLen is the footer length (4) plus the closing magic.
	trailerLen = 4 + len(magic)
)

var crcTable = crc32.MakeTable(crc32.Castagnoli)

// ErrCorrupt is wrapped by every error that says a file is not a complete, undamaged table
// file of this format.
var ErrCorrupt = errors.New("tinystore: damaged or incomplete table file")

func corrupt(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrCorrupt, fmt.Sprintf(format, args...))
}

// Column encodings.
const (
	encInt       byte = 1
	encFloat     byte = 2
	encTextDict  byte = 3
	encTextPlain byte = 4
)

// Column types as stored (independent of tinySQL's own numbering, which is not a file format).
const (
	typeText  byte = 0
	typeInt   byte = 1
	typeFloat byte = 2
)

func storedType(t tsql.ColType) (byte, error) {
	switch t {
	case tsql.TextType:
		return typeText, nil
	case tsql.IntType:
		return typeInt, nil
	case tsql.FloatType:
		return typeFloat, nil
	}
	return 0, fmt.Errorf("tinystore: column type %v is not supported (TEXT, INT and FLOAT are)", t)
}

func tinyType(b byte) (tsql.ColType, bool) {
	switch b {
	case typeText:
		return tsql.TextType, true
	case typeInt:
		return tsql.IntType, true
	case typeFloat:
		return tsql.FloatType, true
	}
	return 0, false
}

// FileName is the name of the file that holds a table. The table name is hex coded so that
// whatever a dataset is called can never name a path (separators, dots, reserved device names)
// and so that two names that differ only in case share a file, as they share a table in SQL.
func FileName(table string) string {
	return hex.EncodeToString([]byte(strings.ToLower(table))) + Ext
}

// Has reports whether dir holds at least one table file.
func Has(dir string) bool {
	m, _ := tableFiles(dir)
	return len(m) > 0
}

// tableFiles lists the table files in dir. It does not use filepath.Glob: Glob also treats
// the directory part as a pattern, so a data folder with [ ] * ? in its path (or a backslash
// on Unix) would find no files, or fail with ErrBadPattern. A folder that does not exist
// holds no tables and is not an error.
func tableFiles(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	var paths []string
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), Ext) {
			continue
		}
		paths = append(paths, filepath.Join(dir, e.Name()))
	}
	sort.Strings(paths)
	return paths, nil
}

// workers is how many columns are encoded or decoded at once. Four is enough to use the
// disk and the memory bus; more only competes with the queries of a running server.
func workers(n int) int {
	w := runtime.GOMAXPROCS(0)
	if w > 4 {
		w = 4
	}
	if w > n {
		w = n
	}
	if w < 1 {
		w = 1
	}
	return w
}

// ── writing ─────────────────────────────────────────────────────────────────

type colMeta struct {
	name   string
	typ    byte
	enc    byte
	offset uint64
	length uint64
}

// Write stores t in dir (created if missing) as <FileName(t.Name)>. The file appears
// atomically: it is written under a temporary name, flushed to disk and renamed, so a crash
// does not publish a partial file. Rename atomicity follows os.Rename on the
// host platform; the directory itself is not fsynced. It returns the file size.
//
// t is only read. Writing runs several columns in parallel; the rows must not be changed
// meanwhile.
func Write(dir string, t *tsql.Table) (int64, error) {
	if t == nil {
		return 0, fmt.Errorf("tinystore: nil table")
	}
	ncols, nrows := len(t.Cols), len(t.Rows)
	if ncols < 1 || ncols > maxCols {
		return 0, fmt.Errorf("tinystore: table %q has %d columns (1 to %d are supported)", t.Name, ncols, maxCols)
	}
	if uint64(nrows) > maxRows {
		return 0, fmt.Errorf("tinystore: table %q has %d rows (at most %d are supported)", t.Name, nrows, maxRows)
	}
	for i, row := range t.Rows {
		if len(row) != ncols {
			return 0, fmt.Errorf("tinystore: table %q row %d has %d values, the table has %d columns", t.Name, i, len(row), ncols)
		}
	}
	types := make([]byte, ncols)
	for c, col := range t.Cols {
		var err error
		if types[c], err = storedType(col.Type); err != nil {
			return 0, fmt.Errorf("column %q: %w", col.Name, err)
		}
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return 0, err
	}
	path := filepath.Join(dir, FileName(t.Name))
	// A temporary name of its own for every call: two writers of the same table (a retry that
	// overlaps a slow first attempt) would otherwise truncate and interleave one shared file,
	// and the one whose rename succeeds could publish a damaged table while reporting
	// success. With separate files each writer produces a complete table and the last
	// rename wins.
	f, err := os.CreateTemp(dir, FileName(t.Name)+".*.tmp")
	if err != nil {
		return 0, err
	}
	tmp := f.Name()
	done := false
	defer func() {
		if !done {
			_ = f.Close()
			_ = os.Remove(tmp)
		}
	}()
	// Large column blocks bypass this buffer; a smaller buffer avoids a 1 MiB
	// allocation per snapshot while coalescing metadata and small blocks.
	w := bufio.NewWriterSize(f, 64<<10)
	pos := uint64(0)
	write := func(b []byte) error {
		n, err := w.Write(b)
		pos += uint64(n)
		return err
	}
	if err := write([]byte(magic)); err != nil {
		return 0, err
	}

	// Columns are encoded a group at a time in parallel and written in column order. A group
	// (not all columns at once) bounds the memory that is held besides the table: the encoded
	// blocks of one group, not of every column.
	metas := make([]colMeta, ncols)
	group := workers(ncols)
	for start := 0; start < ncols; start += group {
		end := start + group
		if end > ncols {
			end = ncols
		}
		blocks := make([][]byte, end-start)
		encs := make([]byte, end-start)
		checksums := make([]uint32, end-start)
		errs := make([]error, end-start)
		var wg sync.WaitGroup
		for c := start; c < end; c++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				blocks[c-start], encs[c-start], errs[c-start] = encodeColumn(t.Rows, c, types[c])
				if errs[c-start] == nil {
					checksums[c-start] = crc32.Checksum(blocks[c-start], crcTable)
				}
			}()
		}
		wg.Wait()
		for c := start; c < end; c++ {
			if err := errs[c-start]; err != nil {
				return 0, fmt.Errorf("tinystore: table %q column %q: %w", t.Name, t.Cols[c].Name, err)
			}
			b := blocks[c-start]
			metas[c] = colMeta{name: t.Cols[c].Name, typ: types[c], enc: encs[c-start], offset: pos, length: uint64(len(b))}
			var sum [crcLen]byte
			binary.LittleEndian.PutUint32(sum[:], checksums[c-start])
			if err := write(b); err != nil {
				return 0, err
			}
			if err := write(sum[:]); err != nil {
				return 0, err
			}
			blocks[c-start] = nil // the block is on its way to disk; let it go
		}
	}

	footer := appendFooter(nil, t.Name, nrows, metas)
	var tail [crcLen + 4]byte
	binary.LittleEndian.PutUint32(tail[:crcLen], crc32.Checksum(footer, crcTable))
	binary.LittleEndian.PutUint32(tail[crcLen:], uint32(len(footer)))
	if err := write(footer); err != nil {
		return 0, err
	}
	if err := write(tail[:]); err != nil {
		return 0, err
	}
	if err := write([]byte(magic)); err != nil {
		return 0, err
	}
	if err := w.Flush(); err != nil {
		return 0, err
	}
	// Flushed to disk before the rename: the copy must survive a restart, and a rename that
	// reaches the disk before the data would leave a complete-looking file with a hole.
	if err := f.Sync(); err != nil {
		return 0, err
	}
	if err := f.Close(); err != nil {
		return 0, err
	}
	if err := os.Rename(tmp, path); err != nil {
		return 0, err
	}
	done = true
	return int64(pos), nil
}

func appendFooter(b []byte, name string, nrows int, metas []colMeta) []byte {
	b = appendString(b, name)
	b = binary.AppendUvarint(b, uint64(len(metas)))
	b = binary.AppendUvarint(b, uint64(nrows))
	for _, m := range metas {
		b = appendString(b, m.name)
		b = append(b, m.typ, m.enc)
		b = binary.AppendUvarint(b, m.offset)
		b = binary.AppendUvarint(b, m.length)
	}
	return b
}

func appendString(b []byte, s string) []byte {
	b = binary.AppendUvarint(b, uint64(len(s)))
	return append(b, s...)
}

// zigzag maps signed to unsigned so that small magnitudes of either sign are short varints.
func zigzag(v int64) uint64 { return uint64(v<<1) ^ uint64(v>>63) }

func unzigzag(u uint64) int64 { return int64(u>>1) ^ -int64(u&1) }

// encodeColumn renders one column as a block.
func encodeColumn(rows [][]any, c int, typ byte) ([]byte, byte, error) {
	switch typ {
	case typeInt:
		b, err := encodeInts(rows, c)
		return b, encInt, err
	case typeFloat:
		b, err := encodeFloats(rows, c)
		return b, encFloat, err
	}
	return encodeText(rows, c)
}

// appendNulls writes the presence flag of a numeric column: 0 when no value is NULL (no
// bitmap follows), else 1 and one bit per row, set for a value that is present.
func appendNulls(b []byte, rows [][]any, c int) []byte {
	hasNull := false
	for _, row := range rows {
		if row[c] == nil {
			hasNull = true
			break
		}
	}
	if !hasNull {
		return append(b, 0)
	}
	b = append(b, 1)
	bits := make([]byte, (len(rows)+7)/8)
	for i, row := range rows {
		if row[c] != nil {
			bits[i>>3] |= 1 << (i & 7)
		}
	}
	return append(b, bits...)
}

func encodeInts(rows [][]any, c int) ([]byte, error) {
	// Three bytes per row accommodates common medium-sized zigzag varints.
	b := make([]byte, 0, len(rows)*3+16)
	b = appendNulls(b, rows, c)
	for i, row := range rows {
		switch v := row[c].(type) {
		case nil:
			b = append(b, 0)
		case int:
			b = binary.AppendUvarint(b, zigzag(int64(v)))
		case int64:
			b = binary.AppendUvarint(b, zigzag(v))
		default:
			return nil, fmt.Errorf("row %d holds %T in an INT column", i, v)
		}
	}
	return b, nil
}

func encodeFloats(rows [][]any, c int) ([]byte, error) {
	b := make([]byte, 0, len(rows)*8+16)
	b = appendNulls(b, rows, c)
	for i, row := range rows {
		switch v := row[c].(type) {
		case nil:
			b = binary.LittleEndian.AppendUint64(b, 0)
		case float64:
			b = binary.LittleEndian.AppendUint64(b, math.Float64bits(v))
		default:
			return nil, fmt.Errorf("row %d holds %T in a FLOAT column", i, v)
		}
	}
	return b, nil
}

// encodeText stores a text column as a dictionary while it has few distinct values and as
// plain text when it has many (see DictMaxDistinct).
func encodeText(rows [][]any, c int) ([]byte, byte, error) {
	dict := map[string]uint32{}
	var order []string
	// Keep dictionary references in their on-disk varint form as we discover them.
	// This avoids a four-byte index per row and a separate encoding pass.
	idx := make([]byte, 0, len(rows))
	useDict := true
	for i, row := range rows {
		switch v := row[c].(type) {
		case nil:
			if useDict {
				idx = append(idx, 0)
			}
		case string:
			if !useDict {
				continue
			}
			n, ok := dict[v]
			if !ok {
				if len(order) >= DictMaxDistinct {
					useDict = false
					dict, order, idx = nil, nil, nil
					continue
				}
				order = append(order, v)
				n = uint32(len(order))
				dict[v] = n
			}
			idx = binary.AppendUvarint(idx, uint64(n))
		default:
			return nil, 0, fmt.Errorf("row %d holds %T in a TEXT column", i, v)
		}
	}
	if useDict {
		size := len(idx) + 16
		for _, s := range order {
			size += len(s) + 2
		}
		b := make([]byte, 0, size)
		b = binary.AppendUvarint(b, uint64(len(order)))
		for _, s := range order {
			b = appendString(b, s)
		}
		b = append(b, idx...)
		return b, encTextDict, nil
	}
	// Sized from the data in one pass: guessing a size per row and growing by append would
	// copy the column several times and hold up to twice its size while doing so, for up to
	// four columns at once (measured: +271 MB over the table at 1 million rows of four
	// all-distinct text columns).
	total := 0
	for _, row := range rows {
		if s, ok := row[c].(string); ok {
			total += len(s)
		}
	}
	b := make([]byte, 0, total+2*len(rows)+16)
	for _, row := range rows {
		if row[c] == nil {
			b = append(b, 0)
			continue
		}
		s := row[c].(string)
		b = binary.AppendUvarint(b, uint64(len(s))+1)
		b = append(b, s...)
	}
	return b, encTextPlain, nil
}

// ── reading ─────────────────────────────────────────────────────────────────

// reader is a cursor over a byte slice that remembers the first thing that went wrong, so a
// decoder can read a whole structure and check once at the end.
type reader struct {
	b   []byte
	pos int
	err error
}

func (r *reader) fail(format string, args ...any) {
	if r.err == nil {
		r.err = corrupt(format, args...)
	}
}

func (r *reader) uvarint() uint64 {
	if r.err != nil {
		return 0
	}
	// Dictionary references and short lengths usually occupy one byte.
	// Avoid the general variable-length decoder in this common case.
	if r.pos < len(r.b) && r.b[r.pos] < 0x80 {
		v := r.b[r.pos]
		r.pos++
		return uint64(v)
	}
	v, n := binary.Uvarint(r.b[r.pos:])
	if n <= 0 {
		r.fail("bad number at byte %d", r.pos)
		return 0
	}
	r.pos += n
	return v
}

func (r *reader) byte1() byte {
	if r.err != nil {
		return 0
	}
	if r.pos >= len(r.b) {
		r.fail("data ends at byte %d", r.pos)
		return 0
	}
	v := r.b[r.pos]
	r.pos++
	return v
}

// take returns the next n bytes without copying.
func (r *reader) take(n uint64) []byte {
	if r.err != nil {
		return nil
	}
	if n > uint64(len(r.b)-r.pos) {
		r.fail("%d bytes wanted at byte %d, %d left", n, r.pos, len(r.b)-r.pos)
		return nil
	}
	out := r.b[r.pos : r.pos+int(n)]
	r.pos += int(n)
	return out
}

func (r *reader) str() string { return string(r.take(r.uvarint())) }

type tableHeader struct {
	name  string
	nrows int
	cols  []colMeta
}

// Read loads the table file at path completely.
func Read(path string) (*tsql.Table, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}
	hdr, err := readHeader(f, fi.Size())
	if err != nil {
		return nil, fmt.Errorf("%s: %w", filepath.Base(path), err)
	}
	t, err := readColumns(f, hdr)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", filepath.Base(path), err)
	}
	return t, nil
}

// readHeader reads and checks everything that is not column data: the magic at both ends, the
// footer and its checksum, and that the blocks it describes tile the file exactly.
func readHeader(f io.ReaderAt, size int64) (*tableHeader, error) {
	minSize := int64(len(magic)*2 + crcLen + 4)
	if size < minSize {
		return nil, corrupt("only %d bytes", size)
	}
	var head [len(magic)]byte
	if _, err := f.ReadAt(head[:], 0); err != nil || string(head[:]) != magic {
		return nil, corrupt("no table file (wrong beginning)")
	}
	var tail [trailerLen]byte
	if _, err := f.ReadAt(tail[:], size-int64(trailerLen)); err != nil || string(tail[4:]) != magic {
		return nil, corrupt("cut off (wrong end)")
	}
	flen := int64(binary.LittleEndian.Uint32(tail[:4]))
	footerStart := size - int64(trailerLen) - crcLen - flen
	if flen < 1 || footerStart < int64(len(magic)) {
		return nil, corrupt("footer length %d does not fit", flen)
	}
	buf := make([]byte, flen+crcLen)
	if _, err := f.ReadAt(buf, footerStart); err != nil {
		return nil, corrupt("footer unreadable: %v", err)
	}
	footer, sum := buf[:flen], binary.LittleEndian.Uint32(buf[flen:])
	if crc32.Checksum(footer, crcTable) != sum {
		return nil, corrupt("footer checksum")
	}
	r := &reader{b: footer}
	h := &tableHeader{name: r.str()}
	ncols := r.uvarint()
	nrows := r.uvarint()
	if r.err != nil {
		return nil, r.err
	}
	if ncols < 1 || ncols > maxCols || nrows > maxRows {
		return nil, corrupt("%d columns and %d rows", ncols, nrows)
	}
	// Rows and the contiguous cell allocation must fit the platform's int.
	if nrows > uint64(int(^uint(0)>>1))/ncols {
		return nil, corrupt("table dimensions exceed platform capacity")
	}
	h.nrows = int(nrows)
	h.cols = make([]colMeta, ncols)
	want := uint64(len(magic)) // where the next block must start
	for i := range h.cols {
		m := &h.cols[i]
		m.name = r.str()
		m.typ, m.enc = r.byte1(), r.byte1()
		m.offset, m.length = r.uvarint(), r.uvarint()
		if r.err != nil {
			return nil, r.err
		}
		if _, ok := tinyType(m.typ); !ok {
			return nil, corrupt("column %q has the unknown type %d", m.name, m.typ)
		}
		if m.offset != want {
			return nil, corrupt("column %q starts at %d, expected %d", m.name, m.offset, want)
		}
		if m.length > uint64(footerStart) || m.offset+m.length+crcLen > uint64(footerStart) {
			return nil, corrupt("column %q runs into the footer", m.name)
		}
		// Every row costs a column at least one byte (a float eight), so a row count that
		// no block can hold is damage, found before anything is allocated for it.
		if min := minBlock(m, h.nrows); m.length < min {
			return nil, corrupt("column %q has %d bytes for %d rows", m.name, m.length, h.nrows)
		}
		want = m.offset + m.length + crcLen
	}
	if r.pos != len(footer) {
		return nil, corrupt("%d unused bytes in the footer", len(footer)-r.pos)
	}
	if want != uint64(footerStart) {
		return nil, corrupt("blocks end at %d, the footer starts at %d", want, footerStart)
	}
	return h, nil
}

// minBlock is the smallest size a block of the column can have for n rows.
func minBlock(m *colMeta, n int) uint64 {
	switch m.enc {
	case encFloat:
		return 1 + 8*uint64(n)
	case encInt:
		return 1 + uint64(n)
	case encTextDict:
		return 1 + uint64(n)
	}
	return uint64(n)
}

func readColumns(f io.ReaderAt, h *tableHeader) (*tsql.Table, error) {
	ncols, nrows := len(h.cols), h.nrows
	cols := make([]tsql.Column, ncols)
	for i, m := range h.cols {
		typ, _ := tinyType(m.typ)
		cols[i] = tsql.Column{Name: m.name, Type: typ}
	}
	// One backing array for all cells and a window of it for each row: two allocations
	// instead of one per row, and the values of a row sit next to each other. A window's
	// capacity is its length, so nothing can append into the neighbour row.
	cells := make([]any, nrows*ncols)
	rows := make([][]any, nrows)
	for i := range rows {
		rows[i] = cells[i*ncols : (i+1)*ncols : (i+1)*ncols]
	}

	errs := make([]error, ncols)
	sem := make(chan struct{}, workers(ncols))
	var wg sync.WaitGroup
	for c := range h.cols {
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer func() { <-sem; wg.Done() }()
			errs[c] = decodeColumn(f, &h.cols[c], rows, c)
		}()
	}
	wg.Wait()
	for c, err := range errs {
		if err != nil {
			return nil, fmt.Errorf("column %q: %w", h.cols[c].name, err)
		}
	}
	t := tsql.NewTable(h.name, cols, false)
	t.Rows = rows
	return t, nil
}

// decodeColumn reads, checks and decodes one block into column c of rows. Text values point
// into the block's own memory (the block is read into a slice nothing else touches and never
// changed afterwards), so a text column costs no allocation per string.
func decodeColumn(f io.ReaderAt, m *colMeta, rows [][]any, c int) error {
	buf := make([]byte, m.length+crcLen)
	if _, err := f.ReadAt(buf, int64(m.offset)); err != nil {
		return corrupt("block unreadable: %v", err)
	}
	b := buf[:m.length]
	if crc32.Checksum(b, crcTable) != binary.LittleEndian.Uint32(buf[m.length:]) {
		return corrupt("checksum")
	}
	r := &reader{b: b}
	switch {
	case m.typ == typeInt && m.enc == encInt:
		decodeInts(r, rows, c)
	case m.typ == typeFloat && m.enc == encFloat:
		decodeFloats(r, rows, c)
	case m.typ == typeText && m.enc == encTextDict:
		decodeDict(r, rows, c)
	case m.typ == typeText && m.enc == encTextPlain:
		decodePlain(r, rows, c)
	default:
		return corrupt("type %d cannot be stored as encoding %d", m.typ, m.enc)
	}
	if r.err != nil {
		return r.err
	}
	if r.pos != len(b) {
		return corrupt("%d unused bytes in the block", len(b)-r.pos)
	}
	return nil
}

// readNulls reads the presence flag and bitmap written by appendNulls; present(i) tells
// whether row i holds a value.
func readNulls(r *reader, n int) func(int) bool {
	switch r.byte1() {
	case 0:
		return func(int) bool { return true }
	case 1:
		bits := r.take(uint64((n + 7) / 8))
		return func(i int) bool { return bits[i>>3]&(1<<(i&7)) != 0 }
	}
	r.fail("bad presence flag")
	return func(int) bool { return false }
}

func decodeInts(r *reader, rows [][]any, c int) {
	present := readNulls(r, len(rows))
	for i := range rows {
		u := r.uvarint()
		if r.err != nil {
			return
		}
		if !present(i) {
			continue
		}
		v := unzigzag(u)
		if int64(int(v)) != v {
			r.fail("value %d does not fit an int on this platform", v)
			return
		}
		rows[i][c] = int(v)
	}
}

func decodeFloats(r *reader, rows [][]any, c int) {
	present := readNulls(r, len(rows))
	data := r.take(8 * uint64(len(rows)))
	if r.err != nil {
		return
	}
	for i := range rows {
		if present(i) {
			rows[i][c] = math.Float64frombits(binary.LittleEndian.Uint64(data[8*i:]))
		}
	}
}

func decodeDict(r *reader, rows [][]any, c int) {
	n := r.uvarint()
	if r.err != nil {
		return
	}
	// Every entry is at least its length byte; a larger count is damage, found before the
	// dictionary is allocated.
	if n > uint64(len(r.b)-r.pos) || n > DictMaxDistinct {
		r.fail("dictionary of %d entries", n)
		return
	}
	// The block is never written again, so strings may share its memory.
	whole := unsafe.String(unsafe.SliceData(r.b), len(r.b))
	dict := make([]any, n)
	for i := range dict {
		l := r.uvarint()
		if r.err != nil {
			return
		}
		if l > uint64(len(r.b)-r.pos) {
			r.fail("dictionary entry of %d bytes", l)
			return
		}
		dict[i] = whole[r.pos : r.pos+int(l)]
		r.pos += int(l)
	}
	for i := range rows {
		k := r.uvarint()
		if r.err != nil {
			return
		}
		switch {
		case k == 0:
		case k <= n:
			rows[i][c] = dict[k-1]
		default:
			r.fail("row %d refers to dictionary entry %d of %d", i, k, n)
			return
		}
	}
}

func decodePlain(r *reader, rows [][]any, c int) {
	whole := unsafe.String(unsafe.SliceData(r.b), len(r.b))
	for i := range rows {
		l := r.uvarint()
		if r.err != nil {
			return
		}
		if l == 0 {
			continue
		}
		l--
		if l > uint64(len(r.b)-r.pos) {
			r.fail("row %d: text of %d bytes", i, l)
			return
		}
		rows[i][c] = whole[r.pos : r.pos+int(l)]
		r.pos += int(l)
	}
}

// ReadDir loads every table file in dir, sorted by file name. One damaged file fails the
// whole call: a copy with a table missing would answer some questions and fail others
// without saying why, and the copy is rebuilt anyway.
func ReadDir(dir string) ([]*tsql.Table, error) {
	paths, err := tableFiles(dir)
	if err != nil {
		return nil, err
	}
	tables := make([]*tsql.Table, 0, len(paths))
	for _, p := range paths {
		t, err := Read(p)
		if err != nil {
			return nil, err
		}
		tables = append(tables, t)
	}
	return tables, nil
}
