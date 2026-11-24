package storage

import (
	"bytes"
	"compress/gzip"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"
)

// ──── File I/O ──────────────────────────────────────────────────────────

// tableEncoder is the common interface between gob.Encoder and json.Encoder
// that the table-file writers need.
type tableEncoder interface {
	Encode(v any) error
}

// tableFileConfig is the part of a DiskBackend's configuration that decides
// how one table file's bytes are produced. A writer captures it once, under
// the backend's lock, and then encodes without holding that lock: encoding a
// large table takes seconds to minutes and must not block the metadata
// operations (ListTableNames, TableExists, IsDirty) that every statement's
// foreign-key and dirty checks go through.
type tableFileConfig struct {
	gzip      bool
	format    diskFileFormat
	encryptor *Encryptor
	// rowBlocks selects the row-block layout (disk_rowblocks.go); it applies to
	// unencrypted GOB files only.
	rowBlocks bool
}

// fileConfig returns the backend's current table-file configuration. The
// caller must not hold b.mu.
func (b *DiskBackend) fileConfig() tableFileConfig {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return tableFileConfig{gzip: b.gzip, format: b.format, encryptor: b.encryptor, rowBlocks: b.rowBlocks.Load()}
}

func (c tableFileConfig) newEncoder(w io.Writer) tableEncoder {
	if c.format == diskFormatJSON {
		return json.NewEncoder(w)
	}
	return gob.NewEncoder(w)
}

// encodeInto GOB- or JSON-encodes dt (gzip-compressed if configured) into w,
// applying the JSON normalization (big.Rat/uuid.UUID -> string; custom
// default-value marshalers keep their representation and the old isolation
// from row data) the JSON format needs. Shared by the encrypted write path,
// which needs the fully encoded bytes in memory before encryption rather than
// streamed straight to a file.
func (c tableFileConfig) encodeInto(w io.Writer, dt diskTable) error {
	var gz *gzip.Writer
	target := w
	if c.gzip {
		gz = borrowDiskGzipWriter(w)
		defer releaseDiskGzipWriter(gz)
		target = gz
	}
	enc := c.newEncoder(target)
	var err error
	if c.format == diskFormatJSON {
		err = enc.Encode(normalizeJSONTable(dt))
	} else {
		err = enc.Encode(dt)
	}
	if gz != nil {
		if closeErr := gz.Close(); err == nil {
			err = closeErr
		}
	}
	return err
}

// encodeTableInto is encodeInto with the backend's own configuration.
func (b *DiskBackend) encodeTableInto(w io.Writer, dt diskTable) error {
	return b.fileConfig().encodeInto(w, dt)
}

// writeTemp writes dt to the temporary file tmp and makes it durable (fsync,
// close). On any error the temporary file is removed and nothing else is
// touched: the table's current file stays as it was. Installing the result is
// installTableFile's job.
func (c tableFileConfig) writeTemp(tmp string, dt diskTable) error {
	if c.rowBlocks && c.encryptor == nil && c.format == diskFormatGob {
		return c.writeRowBlocks(tmp, dt)
	}
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}

	// Encryption needs the complete ciphertext (GCM authenticates the
	// whole message at once, it isn't a true streaming cipher the way
	// gzip/GOB are), so when an encryptor is configured, encode+gzip into
	// an in-memory buffer first, encrypt that buffer's bytes as one blob,
	// and write the result in a single Write call — instead of the
	// unencrypted path's direct stream to the file below. Table files are
	// bounded in size for this engine's use case, so buffering one file's
	// contents in memory is an acceptable trade for correct AEAD usage.
	if c.encryptor != nil {
		var buf bytes.Buffer
		if err := c.encodeInto(&buf, dt); err != nil {
			_ = f.Close()
			_ = os.Remove(tmp)
			return err
		}
		ciphertext, err := c.encryptor.Encrypt(buf.Bytes())
		if err != nil {
			_ = f.Close()
			_ = os.Remove(tmp)
			return fmt.Errorf("encrypt table file: %w", err)
		}
		if _, err := f.Write(ciphertext); err != nil {
			_ = f.Close()
			_ = os.Remove(tmp)
			return err
		}
		if err := f.Sync(); err != nil {
			_ = f.Close()
			_ = os.Remove(tmp)
			return err
		}
		if err := f.Close(); err != nil {
			_ = os.Remove(tmp)
			return err
		}
		return nil
	}

	target, hookErr := testFileTarget(f, tmp)
	if hookErr != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return hookErr
	}
	bw := borrowDiskWriter(target)
	defer releaseDiskWriter(bw)
	var w io.Writer = bw
	var gz *gzip.Writer
	if c.gzip {
		gz = borrowDiskGzipWriter(bw)
		defer releaseDiskGzipWriter(gz)
		w = gz
	}

	enc := c.newEncoder(w)
	var encErr error
	if c.format == diskFormatJSON {
		// JSONMarshal-equivalent normalization (big.Rat/uuid.UUID -> string)
		// applied to Rows before encoding. Custom default-value marshalers
		// retain their representation and the old isolation from row data.
		encErr = enc.Encode(normalizeJSONTable(dt))
	} else {
		encErr = enc.Encode(dt)
	}

	// Close gzip layer first (flushes compressed data to bw)
	if gz != nil {
		if err := gz.Close(); err != nil && encErr == nil {
			encErr = err
		}
	}
	// Flush the bufio layer
	if err := bw.Flush(); err != nil && encErr == nil {
		encErr = err
	}
	// Sync and close the file
	if err := f.Sync(); err != nil && encErr == nil {
		encErr = err
	}
	if err := f.Close(); err != nil && encErr == nil {
		encErr = err
	}
	if encErr != nil {
		_ = os.Remove(tmp)
		return encErr
	}
	return nil
}

// installTableFile atomically installs tmp as path (rename + directory fsync
// for crash safety) and returns the final file's size. tmp must already be
// durably on disk.
func installTableFile(tmp, path string) (int64, error) {
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return 0, err
	}
	if err := syncDir(filepath.Dir(path)); err != nil {
		return 0, err
	}
	info, err := os.Stat(path)
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}

// tableFileWriteTestHook lets a test observe and disturb the unencrypted
// table-file write path: Before runs once the temporary file exists and before
// any data is written (an error fails the write as an I/O error would; blocking
// parks the write with no engine lock held), Wrap wraps the file the bytes go
// to (to fail after N bytes). Nil in production.
type tableFileWriteTestHook struct {
	Before func(tmp string) error
	Wrap   func(tmp string, w io.Writer) io.Writer
}

var tableFileWriteHook atomic.Pointer[tableFileWriteTestHook]

// testFileTarget applies the test hook to the file a write is about to stream
// into, returning the writer to use and the hook's pre-write verdict.
func testFileTarget(f *os.File, tmp string) (io.Writer, error) {
	h := tableFileWriteHook.Load()
	if h == nil {
		return f, nil
	}
	if h.Before != nil {
		if err := h.Before(tmp); err != nil {
			return nil, err
		}
	}
	if h.Wrap != nil {
		return h.Wrap(tmp, f), nil
	}
	return f, nil
}
