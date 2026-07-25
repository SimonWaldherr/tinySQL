// Opening a database: allocating an empty one, and OpenDB's per-mode
// construction of the backend, write-ahead log and checkpoint that a storage
// mode implies. Also loading a GOB checkpoint and reading the watermark that
// says how far it reflects the log.
package storage

import (
	"bufio"
	"compress/gzip"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"
)

// NewDB creates a new empty database catalog with MVCC support.
// The database operates in ModeMemory (pure in-memory) by default.
// Use OpenDB for disk-backed or hybrid storage modes.
func NewDB() *DB {
	return &DB{
		tenants:           map[string]*tenantDB{},
		mvcc:              NewMVCCManager(),
		storageMode:       ModeMemory,
		extensions:        map[string]ExtensionInfo{},
		loadingExtensions: map[string]struct{}{},
	}
}

// applyEncryptionKey enables AES-256-GCM encryption at rest on backend when
// key is non-empty, validating its length immediately with a clear error —
// rather than letting a wrong-size key surface later as an opaque failure
// the first time SaveTable/LoadTable actually tries to use it.
func applyEncryptionKey(backend *DiskBackend, key []byte) error {
	if len(key) == 0 {
		return nil
	}
	enc, err := NewEncryptor(key)
	if err != nil {
		return fmt.Errorf("storage encryption: %w", err)
	}
	backend.SetEncryptor(enc)
	return nil
}

// OpenDB creates or opens a database with the specified storage configuration.
// For ModeMemory this is equivalent to NewDB (with optional save-on-close).
// For ModeDisk/ModeJSON/ModeHybrid/ModeIndex, tables are stored as individual
// files in the configured directory. For ModeWAL, the existing WAL mechanism is
// configured automatically.
func OpenDB(cfg StorageConfig) (*DB, error) {
	// WAL open/recovery currently opens its log read-write and can truncate a
	// torn tail during recovery. Reject it rather than claiming that a
	// read-only open is safe while creating or modifying a WAL sidecar.
	if cfg.ReadOnly && (cfg.Mode == ModeWAL || cfg.Mode == ModeAdvancedWAL) {
		return nil, fmt.Errorf("read-only open is not supported for %s; use a checkpointed disk, index, hybrid, or json artifact", cfg.Mode)
	}
	// Persistent read-only modes must never turn a typo or missing artifact
	// into a newly created directory. NewDiskBackend normally creates its root
	// for import workflows, so validate before it is constructed.
	if cfg.ReadOnly && (cfg.Mode == ModeDisk || cfg.Mode == ModeJSON || cfg.Mode == ModeIndex || cfg.Mode == ModeHybrid || cfg.Mode == ModePagedIndex) {
		info, err := os.Stat(cfg.Path)
		if err != nil {
			return nil, fmt.Errorf("read-only open requires an existing storage directory %q: %w", cfg.Path, err)
		}
		if !info.IsDir() {
			return nil, fmt.Errorf("read-only open requires a storage directory, got %q", cfg.Path)
		}
	}

	db := &DB{
		tenants:     map[string]*tenantDB{},
		mvcc:        NewMVCCManager(),
		storageMode: cfg.Mode,
		config:      &cfg,
	}

	switch cfg.Mode {
	case ModeMemory:
		mb := NewMemoryBackend(cfg.Path)
		mb.setDB(db)
		db.backend = mb
		// If a path is given, try loading an existing GOB file.
		if cfg.Path != "" {
			if loaded, err := loadGOBInto(db, cfg.Path); err != nil {
				return nil, fmt.Errorf("open memory db: %w", err)
			} else if loaded {
				// Update the back-pointer after loading
				mb.setDB(db)
			}
		}

	case ModeWAL:
		if cfg.Path == "" {
			return nil, fmt.Errorf("ModeWAL requires a Path")
		}
		// Load checkpoint if exists
		checkpointLoaded, err := loadGOBInto(db, cfg.Path)
		if err != nil {
			return nil, fmt.Errorf("open wal db: %w", err)
		}
		// Attach WAL
		walCfg := WALConfig{
			Path:               cfg.Path,
			CheckpointEvery:    cfg.CheckpointEvery,
			CheckpointInterval: cfg.CheckpointInterval,
			CheckpointMaxBytes: cfg.CheckpointMaxBytes,
		}
		wal, err := OpenWAL(db, walCfg)
		if err != nil {
			return nil, fmt.Errorf("open wal: %w", err)
		}
		wal.recovery.CheckpointLoaded = checkpointLoaded
		db.attachWAL(wal)
		db.recordRecovery(wal.recovery)

	case ModeAdvancedWAL:
		if cfg.Path == "" {
			return nil, fmt.Errorf("ModeAdvancedWAL requires a Path")
		}
		checkpointPath := cfg.Path + ".checkpoint"
		checkpointLoaded, err := loadGOBInto(db, checkpointPath)
		if err != nil {
			return nil, fmt.Errorf("open advanced wal checkpoint: %w", err)
		}
		walCfg := AdvancedWALConfig{
			Path:               cfg.Path,
			CheckpointPath:     checkpointPath,
			CheckpointEvery:    cfg.CheckpointEvery,
			CheckpointInterval: cfg.CheckpointInterval,
			CheckpointMaxBytes: cfg.CheckpointMaxBytes,
			Compress:           cfg.CompressFiles,
			BufferSize:         64 * 1024,
		}
		wal, err := OpenAdvancedWAL(walCfg)
		if err != nil {
			return nil, fmt.Errorf("open advanced wal: %w", err)
		}
		// Recover pending WAL operations
		recovered, err := wal.Recover(db)
		if err != nil {
			return nil, fmt.Errorf("recover advanced wal: %w", err)
		}
		db.AttachAdvancedWAL(wal)
		db.recordRecovery(RecoveryStatus{
			Mode:                ModeAdvancedWAL,
			Path:                cfg.Path,
			CheckpointLoaded:    checkpointLoaded,
			RecoveredOperations: recovered,
			RecoveredAt:         time.Now(),
		})

	case ModeDisk:
		if cfg.Path == "" {
			return nil, fmt.Errorf("ModeDisk requires a Path")
		}
		backend, err := NewDiskBackend(cfg.Path, cfg.CompressFiles)
		if err != nil {
			return nil, fmt.Errorf("open disk db: %w", err)
		}
		if err := applyEncryptionKey(backend, cfg.EncryptionKey); err != nil {
			return nil, err
		}
		backend.SetReadOnly(cfg.ReadOnly)
		db.backend = backend

	case ModeJSON:
		if cfg.Path == "" {
			return nil, fmt.Errorf("ModeJSON requires a Path")
		}
		backend, err := NewJSONBackend(cfg.Path, cfg.CompressFiles)
		if err != nil {
			return nil, fmt.Errorf("open json db: %w", err)
		}
		if err := applyEncryptionKey(backend, cfg.EncryptionKey); err != nil {
			return nil, err
		}
		backend.SetReadOnly(cfg.ReadOnly)
		db.backend = backend

	case ModeIndex:
		if cfg.Path == "" {
			return nil, fmt.Errorf("ModeIndex requires a Path")
		}
		mem := cfg.MaxMemoryBytes
		if mem <= 0 {
			mem = 64 * 1024 * 1024 // 64 MB
		}
		backend, err := NewHybridBackend(cfg.Path, mem, cfg.CompressFiles, ModeIndex)
		if err != nil {
			return nil, fmt.Errorf("open index db: %w", err)
		}
		if err := applyEncryptionKey(backend.Disk(), cfg.EncryptionKey); err != nil {
			return nil, err
		}
		backend.SetReadOnly(cfg.ReadOnly)
		db.backend = backend

	case ModeHybrid:
		if cfg.Path == "" {
			return nil, fmt.Errorf("ModeHybrid requires a Path")
		}
		mem := cfg.MaxMemoryBytes
		if mem <= 0 {
			mem = 256 * 1024 * 1024 // 256 MB
		}
		backend, err := NewHybridBackend(cfg.Path, mem, cfg.CompressFiles, ModeHybrid)
		if err != nil {
			return nil, fmt.Errorf("open hybrid db: %w", err)
		}
		if err := applyEncryptionKey(backend.Disk(), cfg.EncryptionKey); err != nil {
			return nil, err
		}
		backend.SetReadOnly(cfg.ReadOnly)
		db.backend = backend

	case ModePagedIndex:
		if cfg.Path == "" {
			return nil, fmt.Errorf("ModePagedIndex requires a Path")
		}
		mem := cfg.MaxMemoryBytes
		if mem <= 0 {
			mem = 64 * 1024 * 1024
		}
		backend, err := NewPagedIndexBackend(cfg.Path, mem, cfg.ReadOnly)
		if err != nil {
			return nil, fmt.Errorf("open paged index db: %w", err)
		}
		db.backend = backend

	default:
		return nil, fmt.Errorf("unsupported storage mode: %v", cfg.Mode)
	}

	if err := db.loadBackendCatalog(); err != nil {
		return nil, err
	}

	if cfg.ReadOnly {
		db.SetReadOnly(true)
	}

	return db, nil
}

// loadGOBInto loads a GOB checkpoint file into an existing DB. It returns
// true if data was actually loaded (file existed and was non-empty).
func loadGOBInto(db *DB, filename string) (bool, error) {
	f, err := os.Open(filename)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, nil
		}
		return false, err
	}
	defer func() { _ = f.Close() }()

	var dump []diskTable
	var r io.Reader = bufio.NewReader(f)
	if strings.HasSuffix(strings.ToLower(filename), ".gz") {
		gr, gzErr := gzip.NewReader(r)
		if gzErr != nil {
			return false, gzErr
		}
		defer func() { _ = gr.Close() }()
		r = gr
	}
	dec := gob.NewDecoder(r)
	if err := dec.Decode(&dump); err != nil {
		if errors.Is(err, io.EOF) {
			return false, nil
		}
		return false, err
	}
	for _, dt := range dump {
		_ = db.Put(dt.Tenant, diskToTable(dt))
	}
	loadedCatalog := false
	var dc diskCatalog
	if err := dec.Decode(&dc); err == nil {
		db.setCatalog(diskToCatalog(dc))
		loadedCatalog = true
	} else if !errors.Is(err, io.EOF) {
		return false, err
	}
	return len(dump) > 0 || loadedCatalog, nil
}

// ReadCheckpointWatermark reads the trailing uint64 a checkpoint file may
// carry after its table dump and catalog (see SaveToFile's extra
// parameter) — the LSN/Seq up to which that checkpoint already reflects
// every operation, used by AdvancedWAL/WALManager to skip re-applying
// already-checkpointed WAL records on recovery (see each Checkpoint/Recover
// pair). Returns 0 with no error if the file doesn't exist, is empty, or
// simply predates this watermark (an older checkpoint format) — all of
// which mean "nothing to skip," the safe default.
func ReadCheckpointWatermark(filename string) (uint64, error) {
	f, err := os.Open(filename)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return 0, nil
		}
		return 0, err
	}
	defer func() { _ = f.Close() }()

	var r io.Reader = bufio.NewReader(f)
	if strings.HasSuffix(strings.ToLower(filename), ".gz") {
		gr, gzErr := gzip.NewReader(r)
		if gzErr != nil {
			return 0, gzErr
		}
		defer func() { _ = gr.Close() }()
		r = gr
	}
	dec := gob.NewDecoder(r)
	var dump []diskTable
	if err := dec.Decode(&dump); err != nil {
		return 0, nil // empty or unreadable as a snapshot: nothing to skip
	}
	var dc diskCatalog
	if err := dec.Decode(&dc); err != nil {
		return 0, nil // no catalog section: predates any watermark too
	}
	var watermark uint64
	if err := dec.Decode(&watermark); err != nil {
		return 0, nil // predates the watermark being written: nothing to skip
	}
	return watermark, nil
}
