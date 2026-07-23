package storage

// MemoryBackend is a no-op backend used by ModeMemory. All data lives
// exclusively in the DB's in-memory tenants map; the backend simply reports
// that tables don't exist on any backing store (so the DB never tries to
// load from disk). Save/Sync are intentional no-ops – persistence is the
// caller's responsibility (via SaveToFile or the WAL).
type MemoryBackend struct {
	// savePath is set when the user configures a path even for memory mode.
	// On Close we'll do a final GOB checkpoint to this path.
	savePath string
	db       *DB // back-pointer for Close-time save
}

// NewMemoryBackend creates a MemoryBackend. If savePath is non-empty, Close
// performs a final SaveToFile to that path.
func NewMemoryBackend(savePath string) *MemoryBackend {
	return &MemoryBackend{savePath: savePath}
}

func (m *MemoryBackend) setDB(db *DB) { m.db = db }

// LoadTable reports no durable table because memory mode has no backing store.
func (m *MemoryBackend) LoadTable(_, _ string) (*Table, error) { return nil, nil }

// SaveTable is a no-op because memory mode keeps rows in the owning DB.
func (m *MemoryBackend) SaveTable(_ string, _ *Table) error { return nil }

// DeleteTable is a no-op because memory mode has no durable table files.
func (m *MemoryBackend) DeleteTable(_, _ string) error { return nil }

// ListTableNames reports no durable table names in memory mode.
func (m *MemoryBackend) ListTableNames(_ string) ([]string, error) { return nil, nil }

// TableExists reports false because memory mode has no durable table files.
func (m *MemoryBackend) TableExists(_, _ string) bool { return false }

// Sync is a no-op because memory mode has no durable writes to flush.
func (m *MemoryBackend) Sync() error { return nil }

func (m *MemoryBackend) Close() error {
	if m.savePath != "" && m.db != nil {
		return SaveToFile(m.db, m.savePath)
	}
	return nil
}

func (m *MemoryBackend) Mode() StorageMode { return ModeMemory }

func (m *MemoryBackend) Stats() BackendStats {
	return BackendStats{Mode: ModeMemory}
}
