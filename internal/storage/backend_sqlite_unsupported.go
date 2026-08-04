//go:build !sqliteimport || js || wasm || baremetal

package storage

import "fmt"

// NewSQLiteBackend reports that ModeSQLite requires the sqliteimport build
// tag, mirroring internal/importer/mbtiles_unsupported.go's fallback for the
// same underlying dependency (modernc.org/sqlite).
func NewSQLiteBackend(path string) (*SQLiteBackend, error) {
	return nil, fmt.Errorf("ModeSQLite requires the sqliteimport build tag")
}

// SQLiteBackend is an opaque placeholder without the sqliteimport build tag:
// NewSQLiteBackend above always fails, so no method on it is ever reachable.
type SQLiteBackend struct{}

func (b *SQLiteBackend) LoadTable(tenant, name string) (*Table, error)  { return nil, nil }
func (b *SQLiteBackend) SaveTable(tenant string, t *Table) error        { return nil }
func (b *SQLiteBackend) DeleteTable(tenant, name string) error          { return nil }
func (b *SQLiteBackend) ListTableNames(tenant string) ([]string, error) { return nil, nil }
func (b *SQLiteBackend) TableExists(tenant, name string) bool           { return false }
func (b *SQLiteBackend) Sync() error                                    { return nil }
func (b *SQLiteBackend) Close() error                                   { return nil }
func (b *SQLiteBackend) Mode() StorageMode                              { return ModeSQLite }
func (b *SQLiteBackend) Stats() BackendStats                            { return BackendStats{Mode: ModeSQLite} }
func (b *SQLiteBackend) SetReadOnly(ro bool)                            {}
