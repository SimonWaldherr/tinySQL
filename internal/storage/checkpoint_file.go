// Reading and writing a whole-database snapshot: to a file, a writer, or a byte
// slice. The file variant writes to a temporary file and renames it, so a crash
// during a save leaves the previous snapshot intact rather than a half-written
// one.
package storage

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"encoding/gob"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
)

// SaveToFile writes a snapshot of the database to a file. If the filename
// ends with .gz, the snapshot is gzip-compressed to reduce size.
//
// The snapshot is written atomically: data goes to a temporary file in the
// same directory, is fsynced, and is then renamed over the target. A crash
// mid-checkpoint therefore never corrupts or truncates the previous snapshot.
// SaveToFile writes an atomic snapshot of db (every table plus the catalog)
// to filename, then gob-encodes each value in extra, in order, immediately
// after — letting a caller persist small auxiliary state (e.g. a WAL
// checkpoint's last-applied LSN/Seq watermark, see
// AdvancedWAL.Checkpoint/WALManager.Checkpoint) atomically with the
// snapshot itself, via the same temp-file-then-rename step, rather than a
// separate file whose write could complete independently of this one and
// leave the two inconsistent after a crash. Existing callers that pass no
// extra values are unaffected; the file format is unchanged for them.
func SaveToFile(db *DB, filename string, extra ...any) error {
	db.mu.RLock()
	defer db.mu.RUnlock()

	// Pre-allocate dump slice with estimated capacity
	var totalTables int
	for _, tdb := range db.tenants {
		totalTables += len(tdb.tables)
	}
	dump := make([]diskTable, 0, totalTables)
	for tn, tdb := range db.tenants {
		for _, t := range tdb.tables {
			dump = append(dump, tableToDisk(tn, t))
		}
	}

	if err := os.MkdirAll(filepath.Dir(filename), 0o755); err != nil && !errors.Is(err, os.ErrExist) {
		return err
	}
	// A unique temporary name, not a fixed "<filename>.tmp". Two saves of the
	// same database can overlap — driver autosave on one connection while
	// another checkpoints, or two goroutines calling Sync — and a shared
	// temporary meant they wrote into the same file and then raced to rename it,
	// so the promoted snapshot could be one save's header over another's body.
	f, err := os.CreateTemp(filepath.Dir(filename), filepath.Base(filename)+".tmp*")
	if err != nil {
		return err
	}
	tmp := f.Name()
	fail := func(err error) error {
		_ = f.Close()
		_ = os.Remove(tmp)
		return err
	}

	bw := bufio.NewWriter(f)
	var w io.Writer = bw
	// Enable gzip compression based on file extension.
	var gz *gzip.Writer
	if strings.HasSuffix(strings.ToLower(filename), ".gz") {
		gz = gzip.NewWriter(w)
		w = gz
	}
	enc := gob.NewEncoder(w)
	if err := enc.Encode(dump); err != nil {
		return fail(err)
	}
	if err := enc.Encode(catalogToDisk(db.Catalog())); err != nil {
		return fail(err)
	}
	for _, v := range extra {
		if err := enc.Encode(v); err != nil {
			return fail(err)
		}
	}
	if gz != nil {
		if err := gz.Close(); err != nil {
			return fail(err)
		}
	}
	if err := bw.Flush(); err != nil {
		return fail(err)
	}
	if err := f.Sync(); err != nil {
		return fail(err)
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, filename); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	// Make the rename durable across power loss (no-op on Windows).
	return syncDir(filepath.Dir(filename))
}

// LoadFromFile loads a database snapshot from a file. It auto-detects gzip
// compression based on the .gz suffix and attaches a WAL if a path is given.
func LoadFromFile(filename string) (*DB, error) {
	f, err := os.Open(filename)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return NewDB(), nil
		}
		return nil, err
	}
	defer func() { _ = f.Close() }()
	var dump []diskTable
	var r io.Reader = bufio.NewReader(f)
	if strings.HasSuffix(strings.ToLower(filename), ".gz") {
		gr, gzErr := gzip.NewReader(r)
		if gzErr != nil {
			return nil, gzErr
		}
		defer func() { _ = gr.Close() }()
		r = gr
	}
	dec := gob.NewDecoder(r)
	if err := dec.Decode(&dump); err != nil {
		if errors.Is(err, io.EOF) {
			return NewDB(), nil
		}
		return nil, err
	}
	db := NewDB()
	for _, dt := range dump {
		_ = db.Put(dt.Tenant, diskToTable(dt))
	}
	var dc diskCatalog
	if err := dec.Decode(&dc); err == nil {
		db.setCatalog(diskToCatalog(dc))
	} else if !errors.Is(err, io.EOF) {
		return nil, err
	}
	if filename != "" {
		cfg := WALConfig{Path: filename}
		wal, err := OpenWAL(db, cfg)
		if err != nil {
			return nil, err
		}
		db.attachWAL(wal)
	}
	return db, nil
}

// SaveToWriter writes a snapshot of the database to an arbitrary writer.
// It does not attach or alter WAL configuration.
//
// Like SaveToFile, it gob-encodes each value in extra, in order, immediately
// after the table dump and catalog — letting a caller carry small auxiliary
// state (e.g. a replication watermark LSN, see SnapshotWithWatermark)
// alongside the snapshot in the same encoded stream. Existing callers that
// pass no extra values are unaffected; the encoded format for them is
// unchanged.
func SaveToWriter(db *DB, w io.Writer, extra ...any) error {
	db.mu.RLock()
	defer db.mu.RUnlock()
	// Pre-allocate dump slice with estimated capacity
	var totalTables int
	for _, tdb := range db.tenants {
		totalTables += len(tdb.tables)
	}
	dump := make([]diskTable, 0, totalTables)
	for tn, tdb := range db.tenants {
		for _, t := range tdb.tables {
			dump = append(dump, tableToDisk(tn, t))
		}
	}
	bw := bufio.NewWriter(w)
	enc := gob.NewEncoder(bw)
	if err := enc.Encode(dump); err != nil {
		return err
	}
	if err := enc.Encode(catalogToDisk(db.Catalog())); err != nil {
		return err
	}
	for _, v := range extra {
		if err := enc.Encode(v); err != nil {
			return err
		}
	}
	return bw.Flush()
}

// LoadFromReader loads a database snapshot from an arbitrary reader.
// The returned DB has no WAL attached.
//
// extra mirrors SaveToWriter's extra parameter: each element must be a
// pointer, and is decoded in order from whatever trailing values follow the
// table dump and catalog in the stream (see SaveToWriter). If the stream has
// fewer trailing values than len(extra) — e.g. it was written by an older
// caller that passed no extra values — decoding stops at the first EOF and
// any remaining pointers in extra are left untouched, matching
// ReadCheckpointWatermark's "predates this field: nothing to fill in"
// behavior rather than erroring.
func LoadFromReader(r io.Reader, extra ...any) (*DB, error) {
	dec := gob.NewDecoder(bufio.NewReader(r))
	var dump []diskTable
	if err := dec.Decode(&dump); err != nil {
		if errors.Is(err, io.EOF) {
			return NewDB(), nil
		}
		return nil, err
	}
	db := NewDB()
	for _, dt := range dump {
		_ = db.Put(dt.Tenant, diskToTable(dt))
	}
	var dc diskCatalog
	if err := dec.Decode(&dc); err == nil {
		db.setCatalog(diskToCatalog(dc))
	} else if !errors.Is(err, io.EOF) {
		return nil, err
	}
	for _, v := range extra {
		if err := dec.Decode(v); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, err
		}
	}
	return db, nil
}

// SaveToBytes serializes the database snapshot to a byte slice. extra is
// passed through to SaveToWriter unchanged.
func SaveToBytes(db *DB, extra ...any) ([]byte, error) {
	var buf bytes.Buffer
	if err := SaveToWriter(db, &buf, extra...); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// LoadFromBytes loads a database from a byte slice. extra is passed through
// to LoadFromReader unchanged.
func LoadFromBytes(b []byte, extra ...any) (*DB, error) {
	return LoadFromReader(bytes.NewReader(b), extra...)
}
