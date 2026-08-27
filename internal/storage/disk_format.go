// The on-disk representation of a database: the serializable forms of columns,
// tables and the system catalog, and the conversions to and from them. This is
// the format compatibility surface, so a change here changes what older files
// mean.
package storage

import (
	"bufio"
	"encoding/gob"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
)

type diskColumn struct {
	Name         string
	Type         ColType
	DeclaredType string
	Affinity     SQLiteAffinity
	NotNull      bool
	HasDefault   bool
	DefaultValue any
	Constraint   ConstraintType
	ForeignKey   *ForeignKeyRef
	PointerTable string
}

type diskTable struct {
	Tenant        string
	Name          string
	Cols          []diskColumn
	Rows          [][]any // JSON columns stored as strings
	IsTemp        bool
	Version       int
	StructVersion int
	Indexes       map[string]*SecondaryIndex
	FTSIndexes    map[string]*FTSIndex
	VectorIndexes map[string]*VectorIndex
	Stats         *TableStats
}

type diskCatalog struct {
	Tables       []*CatalogTable
	Columns      map[string][]CatalogColumn
	Views        []*CatalogView
	MViews       []*CatalogMaterializedView
	Dependencies []CatalogDependency
	Indexes      []*CatalogIndex
	Funcs        []*CatalogFunction
	Jobs         []*CatalogJob
	JobRuns      []*CatalogJobHistory
	NextRun      int64
	Triggers     []*CatalogTrigger
	// RBAC carries the users/roles/grants. It must be part of every catalog
	// snapshot, not just the persisted ones: diskCatalog is also the in-memory
	// rollback form used by StatementSnapshot, so omitting RBAC here silently
	// dropped every user on any failed statement — and because enforcement is
	// opt-in via HasUsers, dropping the last user turns authorization OFF
	// instead of denying access. A nil value means "no RBAC state in this
	// snapshot" (older files), which decodes to an empty, disabled-by-default
	// rbacState exactly as before.
	RBAC *diskRBAC
}

// diskRBAC is the serializable form of rbacState. Passwords are stored as the
// same bcrypt hashes rbacState holds in memory; nothing is decrypted or
// re-hashed by a round trip.
type diskRBAC struct {
	Users    []CatalogUser
	Roles    []CatalogRole
	Disabled bool
}

// rbacToDisk snapshots the RBAC state. The returned value shares no memory
// with c, so a later mutation cannot reach back into a snapshot.
func rbacToDisk(r *rbacState) *diskRBAC {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := &diskRBAC{
		Users:    make([]CatalogUser, 0, len(r.users)),
		Roles:    make([]CatalogRole, 0, len(r.roles)),
		Disabled: r.disabled,
	}
	for _, u := range r.users {
		if u == nil {
			continue
		}
		cp := *u
		cp.Roles = append([]string(nil), u.Roles...)
		out.Users = append(out.Users, cp)
	}
	for _, role := range r.roles {
		if role == nil {
			continue
		}
		cp := *role
		cp.Grants = append([]Grant(nil), role.Grants...)
		out.Roles = append(out.Roles, cp)
	}
	sort.Slice(out.Users, func(i, j int) bool { return out.Users[i].Name < out.Users[j].Name })
	sort.Slice(out.Roles, func(i, j int) bool { return out.Roles[i].Name < out.Roles[j].Name })
	return out
}

// diskToRBAC rebuilds rbacState from a snapshot. A nil snapshot yields a
// fresh, empty state so pre-RBAC catalog files keep loading unchanged.
func diskToRBAC(d *diskRBAC) *rbacState {
	r := newRBACState()
	if d == nil {
		return r
	}
	r.disabled = d.Disabled
	for i := range d.Users {
		cp := d.Users[i]
		cp.Roles = append([]string(nil), d.Users[i].Roles...)
		r.users[strings.ToLower(cp.Name)] = &cp
	}
	for i := range d.Roles {
		cp := d.Roles[i]
		cp.Grants = append([]Grant(nil), d.Roles[i].Grants...)
		r.roles[strings.ToLower(cp.Name)] = &cp
	}
	return r
}

func catalogToDisk(c *CatalogManager) diskCatalog {
	if c == nil {
		return diskCatalog{Columns: make(map[string][]CatalogColumn), NextRun: 1}
	}
	c.mu.RLock()
	defer c.mu.RUnlock()

	dc := diskCatalog{
		Tables:       make([]*CatalogTable, 0, len(c.tables)),
		Columns:      make(map[string][]CatalogColumn, len(c.columns)),
		Views:        make([]*CatalogView, 0, len(c.views)),
		MViews:       make([]*CatalogMaterializedView, 0, len(c.mviews)),
		Dependencies: make([]CatalogDependency, 0),
		Indexes:      make([]*CatalogIndex, 0, len(c.indexes)),
		Funcs:        make([]*CatalogFunction, 0, len(c.funcs)),
		Jobs:         make([]*CatalogJob, 0, len(c.jobs)),
		JobRuns:      make([]*CatalogJobHistory, 0, len(c.jobRuns)),
		NextRun:      c.nextRun,
		Triggers:     make([]*CatalogTrigger, 0, len(c.triggers)),
	}
	if dc.NextRun == 0 {
		dc.NextRun = 1
	}
	for _, t := range c.tables {
		cp := *t
		dc.Tables = append(dc.Tables, &cp)
	}
	for k, cols := range c.columns {
		cp := make([]CatalogColumn, len(cols))
		copy(cp, cols)
		dc.Columns[k] = cp
	}
	for _, v := range c.views {
		cp := *v
		dc.Views = append(dc.Views, &cp)
	}
	for _, mv := range c.mviews {
		cp := *mv
		dc.MViews = append(dc.MViews, &cp)
	}
	for _, deps := range c.dependencies {
		dc.Dependencies = append(dc.Dependencies, deps...)
	}
	for _, idx := range c.indexes {
		cp := *idx
		cp.Columns = append([]string(nil), idx.Columns...)
		dc.Indexes = append(dc.Indexes, &cp)
	}
	for _, f := range c.funcs {
		cp := *f
		if f.ArgTypes != nil {
			cp.ArgTypes = append([]string(nil), f.ArgTypes...)
		}
		dc.Funcs = append(dc.Funcs, &cp)
	}
	for _, j := range c.jobs {
		cp := *j
		dc.Jobs = append(dc.Jobs, &cp)
	}
	for _, run := range c.jobRuns {
		cp := *run
		dc.JobRuns = append(dc.JobRuns, &cp)
	}
	for _, t := range c.triggers {
		cp := *t
		dc.Triggers = append(dc.Triggers, &cp)
	}
	dc.RBAC = rbacToDisk(c.rbac)
	sortDiskCatalog(&dc)
	return dc
}

// sortDiskCatalog puts every slice in a deterministic order. The slices are
// built by ranging over maps, so without this the same catalog encodes
// differently on each call — which makes two snapshots of identical content
// compare unequal (see CatalogSnapshot.Equal, used to decide whether a
// transaction really changed the catalog) and makes checkpoint files differ
// byte-for-byte between runs of the same data.
func sortDiskCatalog(dc *diskCatalog) {
	sort.Slice(dc.Tables, func(i, j int) bool {
		if dc.Tables[i].Schema != dc.Tables[j].Schema {
			return dc.Tables[i].Schema < dc.Tables[j].Schema
		}
		return dc.Tables[i].Name < dc.Tables[j].Name
	})
	sort.Slice(dc.Views, func(i, j int) bool {
		if dc.Views[i].Schema != dc.Views[j].Schema {
			return dc.Views[i].Schema < dc.Views[j].Schema
		}
		return dc.Views[i].Name < dc.Views[j].Name
	})
	sort.Slice(dc.MViews, func(i, j int) bool {
		if dc.MViews[i].Schema != dc.MViews[j].Schema {
			return dc.MViews[i].Schema < dc.MViews[j].Schema
		}
		return dc.MViews[i].Name < dc.MViews[j].Name
	})
	sort.Slice(dc.Dependencies, func(i, j int) bool {
		a, b := dc.Dependencies[i], dc.Dependencies[j]
		if a.Schema != b.Schema {
			return a.Schema < b.Schema
		}
		if a.ObjectName != b.ObjectName {
			return a.ObjectName < b.ObjectName
		}
		if a.DependsOnSchema != b.DependsOnSchema {
			return a.DependsOnSchema < b.DependsOnSchema
		}
		if a.DependsOnName != b.DependsOnName {
			return a.DependsOnName < b.DependsOnName
		}
		return a.DependencyType < b.DependencyType
	})
	sort.Slice(dc.Indexes, func(i, j int) bool {
		a, b := dc.Indexes[i], dc.Indexes[j]
		if a.Tenant != b.Tenant {
			return a.Tenant < b.Tenant
		}
		if a.Schema != b.Schema {
			return a.Schema < b.Schema
		}
		return a.Name < b.Name
	})
	sort.Slice(dc.Funcs, func(i, j int) bool {
		if dc.Funcs[i].Schema != dc.Funcs[j].Schema {
			return dc.Funcs[i].Schema < dc.Funcs[j].Schema
		}
		return dc.Funcs[i].Name < dc.Funcs[j].Name
	})
	sort.Slice(dc.Jobs, func(i, j int) bool { return dc.Jobs[i].Name < dc.Jobs[j].Name })
	sort.Slice(dc.Triggers, func(i, j int) bool { return dc.Triggers[i].Name < dc.Triggers[j].Name })
}

// CatalogSnapshot is an opaque, comparable copy of a catalog's contents. It
// exists so the SQL driver can tell "this transaction created a view" from
// "this transaction ran an INSERT that happened to touch catalog bookkeeping",
// which a revision counter alone cannot distinguish.
type CatalogSnapshot struct {
	dc diskCatalog
}

// SnapshotCatalog copies this database's catalog contents for later comparison.
func (db *DB) SnapshotCatalog() CatalogSnapshot {
	if db == nil {
		return CatalogSnapshot{dc: catalogToDisk(nil)}
	}
	return CatalogSnapshot{dc: catalogToDisk(db.Catalog())}
}

// Equal reports whether two snapshots hold the same catalog contents.
func (s CatalogSnapshot) Equal(other CatalogSnapshot) bool {
	return reflect.DeepEqual(s.dc, other.dc)
}

func diskToCatalog(dc diskCatalog) *CatalogManager {
	c := NewCatalogManager()
	for _, t := range dc.Tables {
		if t == nil {
			continue
		}
		cp := *t
		c.tables[cp.Schema+"."+cp.Name] = &cp
	}
	for k, cols := range dc.Columns {
		cp := make([]CatalogColumn, len(cols))
		copy(cp, cols)
		c.columns[k] = cp
	}
	for _, v := range dc.Views {
		if v == nil {
			continue
		}
		cp := *v
		c.views[cp.Schema+"."+cp.Name] = &cp
	}
	for _, mv := range dc.MViews {
		if mv == nil {
			continue
		}
		cp := *mv
		c.mviews[cp.Schema+"."+cp.Name] = &cp
	}
	for _, dep := range dc.Dependencies {
		key := dep.Schema + "." + dep.ObjectName
		c.dependencies[key] = append(c.dependencies[key], dep)
	}
	for _, idx := range dc.Indexes {
		if idx == nil {
			continue
		}
		cp := *idx
		cp.Columns = append([]string(nil), idx.Columns...)
		if cp.Tenant == "" {
			// Snapshots created before tenant-scoped index metadata cannot
			// identify the owning tenant. Preserve them for administrative
			// inspection, but do not expose them to a tenant by guessing.
			c.indexes[legacyCatalogIndexKey(cp.Schema, cp.Name)] = &cp
			continue
		}
		cp.Tenant = normalizeCatalogTenant(cp.Tenant)
		c.indexes[catalogIndexKey(cp.Tenant, cp.Schema, cp.Name)] = &cp
	}
	for _, f := range dc.Funcs {
		if f == nil {
			continue
		}
		cp := *f
		if f.ArgTypes != nil {
			cp.ArgTypes = append([]string(nil), f.ArgTypes...)
		}
		c.funcs[cp.Schema+"."+cp.Name] = &cp
	}
	for _, j := range dc.Jobs {
		if j == nil {
			continue
		}
		cp := *j
		c.jobs[cp.Name] = &cp
	}
	for _, run := range dc.JobRuns {
		if run == nil {
			continue
		}
		cp := *run
		c.jobRuns = append(c.jobRuns, &cp)
		if cp.RunID >= c.nextRun {
			c.nextRun = cp.RunID + 1
		}
	}
	if dc.NextRun > c.nextRun {
		c.nextRun = dc.NextRun
	}
	if c.nextRun == 0 {
		c.nextRun = 1
	}
	for _, t := range dc.Triggers {
		if t == nil {
			continue
		}
		cp := *t
		c.triggers[cp.Name] = &cp
		c.addTriggerEventLocked(&cp)
	}
	c.rbac = diskToRBAC(dc.RBAC)
	return c
}

func (db *DB) backendCatalogPath() (string, bool) {
	if db == nil || db.config == nil || db.config.Path == "" {
		return "", false
	}
	switch db.storageMode {
	case ModeDisk, ModeHybrid, ModeIndex, ModeJSON:
		return filepath.Join(db.config.Path, ".catalog.gob"), true
	case ModeSQLite:
		// Path is the .sqlite file itself, not a directory, so the catalog
		// sidecar sits next to it rather than inside it.
		return db.config.Path + ".catalog.gob", true
	default:
		return "", false
	}
}

func (db *DB) loadBackendCatalog() error {
	path, ok := db.backendCatalogPath()
	if !ok {
		return nil
	}
	f, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	defer func() { _ = f.Close() }()

	var dc diskCatalog
	if err := gob.NewDecoder(bufio.NewReader(f)).Decode(&dc); err != nil {
		if errors.Is(err, io.EOF) {
			return nil
		}
		return err
	}
	db.setCatalog(diskToCatalog(dc))
	return nil
}

func (db *DB) saveBackendCatalog() error {
	if db.IsReadOnly() {
		return nil
	}
	path, ok := db.backendCatalogPath()
	if !ok {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	bw := bufio.NewWriter(f)
	encErr := gob.NewEncoder(bw).Encode(catalogToDisk(db.Catalog()))
	flushErr := bw.Flush()
	closeErr := f.Close()
	if encErr != nil {
		return encErr
	}
	if flushErr != nil {
		return flushErr
	}
	return closeErr
}
