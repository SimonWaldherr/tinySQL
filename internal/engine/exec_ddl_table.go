package engine

import (
	"fmt"
	"strings"
	"time"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// executeCreateTable handles ordinary, virtual, and CREATE TABLE AS SELECT
// tables. The SELECT is dispatched without reacquiring Execute's lock.
func executeCreateTable(env ExecEnv, s *CreateTable) (*ResultSet, error) {
	if s.IfNotExists {
		if _, err := env.db.Get(env.tenant, s.Name); err == nil {
			return nil, nil
		}
	}
	if s.VirtualTable && s.Using == "fts" {
		return executeCreateFTSTable(env, s)
	}
	if s.AsSelect == nil {
		return nil, env.db.Put(env.tenant, storage.NewTable(s.Name, s.Cols, s.IsTemp))
	}
	rs, err := execStmt(env, s.AsSelect)
	if err != nil {
		return nil, err
	}
	cols := make([]storage.Column, len(rs.Cols))
	for i, c := range rs.Cols {
		typ := storage.TextType
		if len(rs.Rows) > 0 {
			typ = inferType(rs.Rows[0][strings.ToLower(c)])
		}
		cols[i] = storage.Column{Name: c, Type: typ}
	}
	t := storage.NewTable(s.Name, cols, s.IsTemp)
	for _, r := range rs.Rows {
		row := make([]any, len(cols))
		for i, c := range cols {
			row[i] = r[strings.ToLower(c.Name)]
		}
		t.Rows = append(t.Rows, row)
	}
	return nil, env.db.Put(env.tenant, t)
}

func executeDropTable(env ExecEnv, s *DropTable) (*ResultSet, error) {
	t, err := env.db.Get(env.tenant, s.Name)
	if err != nil {
		if s.IfExists {
			return nil, nil
		}
	} else {
		invalidateConstraintIndexes(t)
		tenant := env.tenant
		if tenant == "" {
			tenant = "default"
		}
		purgeVectorCachesFor(tenant, t.Name)
		// Also drop the query-result cache and FTS caches for this table.
		// Otherwise a DROP + recreate can serve stale top-K row IDs (wrong
		// rows, or a panic on an out-of-range index) and leak FTS memory.
		purgeVecQueryCacheFor(tenant, t.Name)
		purgeFTSCachesFor(tenant, t.Name)
		env.db.Catalog().DeleteIndexesForTable(s.Name)
	}
	return nil, env.db.Drop(env.tenant, s.Name)
}

func executeCreateIndex(env ExecEnv, s *CreateIndex) (*ResultSet, error) {
	schema, name := splitObjectName(s.Name)
	if _, exists := env.db.Catalog().GetIndex(schema, name); exists {
		if s.IfNotExists {
			return nil, nil
		}
		return nil, fmt.Errorf("index %q already exists", s.Name)
	}
	t, err := env.db.Get(env.tenant, s.Table)
	if err != nil {
		return nil, err
	}
	for _, col := range s.Columns {
		if _, err := t.ColIndex(col); err != nil {
			return nil, err
		}
	}
	if err := t.CreateSecondaryIndex(name, s.Columns, s.Unique); err != nil {
		return nil, err
	}
	if err := env.db.Catalog().RegisterIndex(&storage.CatalogIndex{Schema: schema, Name: name, Table: s.Table, Columns: append([]string(nil), s.Columns...), Unique: s.Unique, CreatedAt: time.Now()}); err != nil {
		t.DropSecondaryIndex(name)
		return nil, err
	}
	t.Version++
	t.MarkDirtyFrom(-1)
	return nil, nil
}

func executeDropIndex(env ExecEnv, s *DropIndex) (*ResultSet, error) {
	schema, name := splitObjectName(s.Name)
	idx, exists := env.db.Catalog().GetIndex(schema, name)
	if !exists {
		if s.IfExists {
			return nil, nil
		}
		return nil, fmt.Errorf("index %q not found", s.Name)
	}
	if s.Table != "" && !strings.EqualFold(idx.Table, s.Table) {
		return nil, fmt.Errorf("index %q is on table %q, not %q", s.Name, idx.Table, s.Table)
	}
	if t, err := env.db.Get(env.tenant, idx.Table); err == nil {
		t.DropSecondaryIndex(name)
		t.Version++
		t.MarkDirtyFrom(-1)
	}
	return nil, env.db.Catalog().DeleteIndex(schema, name)
}
