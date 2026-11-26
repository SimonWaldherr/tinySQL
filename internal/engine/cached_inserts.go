package engine

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func isCachedInsertCommand(stmt Statement) bool {
	switch s := stmt.(type) {
	case *Insert:
		return s.Cached
	case *FlushInserts, *DiscardInserts:
		return true
	default:
		return false
	}
}

func insertCacheOwner(ctx context.Context) string {
	user, _ := UserFromContext(ctx)
	return strings.ToLower(user)
}

func insertCacheCount(column string, count int) *ResultSet {
	return &ResultSet{Cols: []string{column}, Rows: []Row{{column: count}}}
}

func checkInsertStageSchema(target, stage *storage.Table) error {
	if stage != nil && !reflect.DeepEqual(target.Cols, stage.Cols) {
		return fmt.Errorf("cached inserts for %q have a different column schema; discard the pending rows first", target.Name)
	}
	return nil
}

func executeCachedInsert(env ExecEnv, s *Insert) (*ResultSet, error) {
	if len(s.Returning) != 0 || s.OnConflict != nil || s.OnConflictDoNothing {
		return nil, fmt.Errorf("INSERT CACHED does not support RETURNING or ON CONFLICT")
	}
	owner := insertCacheOwner(env.ctx)
	stage := env.db.StagedInsertTable(env.tenant, s.Table, owner)
	target := stage
	var err error
	if stage == nil {
		target, err = env.db.Get(env.tenant, s.Table)
		if err != nil {
			return nil, err
		}
	} else if resident := storage.ResidentTable(env.db, env.tenant, s.Table); resident != nil {
		// Reuse the cached schema when the destination has been evicted.
		// FLUSH always validates the current schema before appending.
		target = resident
	}
	if err := checkInsertStageSchema(target, stage); err != nil {
		return nil, err
	}

	var columnBuf [16]int
	columns := columnBuf[:0]
	width := len(target.Cols)
	if len(s.Cols) != 0 {
		var seenBuf [64]bool
		seen := seenBuf[:min(width, len(seenBuf))]
		if width > len(seen) {
			seen = make([]bool, width)
		}
		for _, name := range s.Cols {
			index, err := target.ColIndex(name)
			if err != nil {
				return nil, err
			}
			if seen[index] {
				return nil, fmt.Errorf("duplicate insert column %q", name)
			}
			seen[index] = true
			columns = append(columns, index)
		}
		width = len(columns)
	}

	source := insertRowSource{values: s.Rows}
	if s.Select != nil {
		if err := checkPermission(env.ctx, env.db, s.Select); err != nil {
			return nil, err
		}
		source, err = executeInsertSource(env, s.Select)
		if err != nil {
			return nil, err
		}
	}
	rowCount := source.len()
	if rowCount == 0 {
		return insertCacheCount("cached", 0), nil
	}

	rows := make([][]any, 0, rowCount)
	arena := dmlRowArena{rowsPerBlock: min(rowCount, 64)}
	var defaults []insertColumnDefault
	hasDefaults := len(s.Cols) != 0 && tableHasDefaults(target.Cols)
	if hasDefaults {
		defaults = planInsertDefaults(target.Cols)
	}
	for i := range rowCount {
		if err := checkCtx(env.ctx); err != nil {
			return nil, err
		}
		values := source.at(i)
		if len(values) != width {
			return nil, fmt.Errorf("INSERT CACHED expects %d values, got %d", width, len(values))
		}
		row := arena.row(len(target.Cols))
		if hasDefaults {
			if defaults != nil {
				err = applyInsertDefaults(row, target.Cols, defaults)
			} else {
				err = applyColumnDefaults(row, target.Cols)
			}
			if err != nil {
				return nil, err
			}
			for j, v := range row {
				row[j] = storage.CloneRowCell(v)
			}
		}
		for j, expr := range values {
			index := j
			if len(columns) != 0 {
				index = columns[j]
			}
			value, err := evalExpr(env, expr, nil)
			if err != nil {
				return nil, err
			}
			col := target.Cols[index]
			cell, err := coerceColumnValue(value, col)
			if err != nil {
				return nil, fmt.Errorf("column %q: %w", target.Cols[index].Name, err)
			}
			// Native BLOB and strict JSON coercion already own their result.
			// Other conversions may pass through a mutable input cell.
			if !immutableInsertDefault(value) && !col.StrictJSON && (col.Type != storage.BlobType || col.Affinity != storage.AffinityDefault) {
				cell = storage.CloneRowCell(cell)
			}
			row[index] = cell
		}
		rows = append(rows, row)
	}
	if err := checkCtx(env.ctx); err != nil {
		return nil, err
	}
	if err := env.db.AppendStagedInsertRows(env.tenant, owner, target, rows); err != nil {
		return nil, err
	}
	return insertCacheCount("cached", len(rows)), nil
}

// Publishing the staged batch is the last step of enqueue. Neither private
// operation needs a live-table rollback point or WAL.
func executeInsertBufferOnly(ctx context.Context, db *storage.DB, tenant string, stmt Statement) (rs *ResultSet, err error) {
	defer func() { recordAudit(ctx, db, tenant, stmt, err) }()
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("internal error executing statement: %v", r)
		}
	}()
	if db.IsReadOnly() {
		return nil, rejectIfMutating(stmt)
	}
	if err := checkCtx(ctx); err != nil {
		return nil, err
	}
	if discard, ok := stmt.(*DiscardInserts); ok {
		return insertCacheCount("discarded", db.DiscardStagedInserts(tenant, discard.Table, insertCacheOwner(ctx))), nil
	}
	subqueries := newSubqueryResultCache()
	defer releaseSubqueryResultCache(subqueries)
	env := ExecEnv{ctx: ctx, db: db, tenant: tenant, now: time.Now(), subqueryCache: subqueries}
	return executeCachedInsert(env, stmt.(*Insert))
}
