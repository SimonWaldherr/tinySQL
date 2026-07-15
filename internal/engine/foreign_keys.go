// Package engine: foreign key referential action enforcement.
//
// Before this, FOREIGN KEY constraints only protected the child side: an
// INSERT/UPDATE into a table with a FOREIGN KEY column checked that the
// referenced row existed (validateRowConstraints in exec.go), but nothing
// checked the parent side — DELETE (or UPDATE of a referenced column) on the
// parent table silently orphaned any referencing child rows, regardless of
// an ON DELETE/ON UPDATE clause (which didn't even parse). This file adds
// that missing half: RESTRICT (the default when no clause is given, matching
// how tinySQL checks constraints immediately rather than deferring them),
// CASCADE, and SET NULL.
package engine

import (
	"fmt"
	"strings"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// fkReference is one column, in one table, whose FOREIGN KEY points at a
// specific (parent table, parent column) pair.
type fkReference struct {
	childTable  *storage.Table
	childColIdx int
	action      storage.ReferentialAction
}

// fkChange describes what happened to one specific parent-column value:
// either it was deleted outright, or it was updated to newVal (which may
// itself be nil, i.e. changed to NULL — distinct from "no change").
type fkChange struct {
	newVal  any
	deleted bool
}

// foreignKeyActionState follows the referential-action path within one
// statement. A key is kept active until the action returns; revisiting the
// same child column/value means a cascade cycle rather than a new action.
// It is intentionally statement-local (via ExecEnv), not global, so tenants
// and concurrent databases never share mutable state.
type foreignKeyActionState struct {
	active map[fkActionKey]struct{}
}

type fkActionKey struct {
	table  *storage.Table
	column int
	value  string
}

// tenantHasAnyForeignKeys reports whether any table in the tenant declares a
// FOREIGN KEY column at all. DELETE/UPDATE call this first so the common
// case (no foreign keys anywhere) pays no cost beyond one cheap metadata
// scan, and can keep using their existing raw fast paths.
func tenantHasAnyForeignKeys(env ExecEnv) bool {
	for _, t := range env.db.ListTables(env.tenant) {
		for _, c := range t.Cols {
			if c.Constraint == storage.ForeignKey && c.ForeignKey != nil {
				return true
			}
		}
	}
	return false
}

// findReferencingForeignKeys returns every column, across every table in the
// tenant, whose FOREIGN KEY targets (parentTable, parentCol).
func findReferencingForeignKeys(env ExecEnv, parentTable, parentCol string, forUpdate bool) []fkReference {
	var refs []fkReference
	for _, t := range env.db.ListTables(env.tenant) {
		for i, c := range t.Cols {
			if c.Constraint != storage.ForeignKey || c.ForeignKey == nil {
				continue
			}
			if !strings.EqualFold(c.ForeignKey.Table, parentTable) || !strings.EqualFold(c.ForeignKey.Column, parentCol) {
				continue
			}
			action := c.ForeignKey.OnDelete
			if forUpdate {
				action = c.ForeignKey.OnUpdate
			}
			refs = append(refs, fkReference{childTable: t, childColIdx: i, action: action})
		}
	}
	return refs
}

// fkOnDeleteCheckAllColumns must be called BEFORE removing deletedRows from
// t, for every DELETE regardless of which internal fast/slow path is taking
// it. Any column could be the target of a foreign key from another table,
// so every column is checked. Returns an error (and performs no mutation at
// all, on any table) if a RESTRICT/NO ACTION reference blocks the delete;
// otherwise performs any CASCADE deletes / SET NULL updates on referencing
// tables and returns nil, in which case the caller may proceed to remove
// deletedRows from t.
func fkOnDeleteCheckAllColumns(env ExecEnv, t *storage.Table, deletedRows [][]any) error {
	if len(deletedRows) == 0 || !tenantHasAnyForeignKeys(env) {
		return nil
	}
	for colIdx, col := range t.Cols {
		changes := make(map[any]fkChange)
		for _, r := range deletedRows {
			if colIdx >= len(r) || r[colIdx] == nil {
				continue
			}
			changes[comparableKeyPart(r[colIdx])] = fkChange{deleted: true}
		}
		if len(changes) == 0 {
			continue
		}
		refs := findReferencingForeignKeys(env, t.Name, col.Name, false)
		for _, ref := range refs {
			if err := applyOneForeignKeyReference(env, ref, changes); err != nil {
				return err
			}
		}
	}
	return nil
}

// fkOnUpdateCheckColumn must be called BEFORE committing a value change in
// parentColIdx from oldVal to newVal for one or more rows of t (UPDATE). Like
// fkOnDeleteCheckAllColumns, it performs no mutation and returns an error if
// a RESTRICT/NO ACTION reference blocks the change; otherwise it applies any
// CASCADE/SET NULL updates on referencing tables and returns nil.
func fkOnUpdateCheckColumn(env ExecEnv, t *storage.Table, parentColIdx int, changes map[any]fkChange) error {
	if len(changes) == 0 || !tenantHasAnyForeignKeys(env) {
		return nil
	}
	refs := findReferencingForeignKeys(env, t.Name, t.Cols[parentColIdx].Name, true)
	for _, ref := range refs {
		if err := applyOneForeignKeyReference(env, ref, changes); err != nil {
			return err
		}
	}
	return nil
}

// applyOneForeignKeyReference performs ref's action (RESTRICT/NO ACTION,
// CASCADE, or SET NULL) for the given set of parent-value changes against
// ref's child table/column.
func applyOneForeignKeyReference(env ExecEnv, ref fkReference, changes map[any]fkChange) error {
	var leave func()
	if ref.action == storage.Cascade || ref.action == storage.SetNull {
		var err error
		env, leave, err = enterForeignKeyAction(env, ref, changes)
		if err != nil {
			return err
		}
		defer leave()
	}
	switch ref.action {
	case storage.Cascade:
		return cascadeForeignKeyReference(env, ref, changes)
	case storage.SetNull:
		return setNullForeignKeyReference(env, ref, changes)
	default: // Restrict, NoAction
		return restrictForeignKeyReference(ref, changes)
	}
}

func enterForeignKeyAction(env ExecEnv, ref fkReference, changes map[any]fkChange) (ExecEnv, func(), error) {
	if env.foreignKeyActions == nil {
		env.foreignKeyActions = &foreignKeyActionState{active: make(map[fkActionKey]struct{})}
	}
	keys := make([]fkActionKey, 0, len(changes))
	for value := range changes {
		key := fkActionKey{table: ref.childTable, column: ref.childColIdx, value: fmtKeyPart(value)}
		if _, active := env.foreignKeyActions.active[key]; active {
			return env, nil, fmt.Errorf("FOREIGN KEY cascade cycle detected at table %q column %q", ref.childTable.Name, ref.childTable.Cols[ref.childColIdx].Name)
		}
		keys = append(keys, key)
	}
	for _, key := range keys {
		env.foreignKeyActions.active[key] = struct{}{}
	}
	return env, func() {
		for _, key := range keys {
			delete(env.foreignKeyActions.active, key)
		}
	}, nil
}

// restrictForeignKeyReference blocks the parent-side change if any child row
// still references one of the changed values. This is the default when no
// ON DELETE/ON UPDATE clause is given, closing the gap where DELETE
// previously orphaned children with no warning at all.
func restrictForeignKeyReference(ref fkReference, changes map[any]fkChange) error {
	child := ref.childTable
	for _, r := range child.Rows {
		if ref.childColIdx >= len(r) || r[ref.childColIdx] == nil {
			continue
		}
		if _, ok := changes[comparableKeyPart(r[ref.childColIdx])]; ok {
			return fmt.Errorf("FOREIGN KEY constraint violation: table %q still has row(s) referencing this value via column %q; add ON DELETE CASCADE or ON DELETE SET NULL to the foreign key to allow this",
				child.Name, child.Cols[ref.childColIdx].Name)
		}
	}
	return nil
}

// setNullForeignKeyReference nulls out the referencing column in every
// matching child row, for both deletions and updates. It deliberately uses
// the same row/index/WAL maintenance as executeUpdate: referential actions
// are still real table updates, even though they do not run SQL triggers.
func setNullForeignKeyReference(env ExecEnv, ref fkReference, changes map[any]fkChange) error {
	child := ref.childTable
	matchingRows := make([]int, 0)
	for i, r := range child.Rows {
		if ref.childColIdx >= len(r) || r[ref.childColIdx] == nil {
			continue
		}
		if _, ok := changes[comparableKeyPart(r[ref.childColIdx])]; ok {
			matchingRows = append(matchingRows, i)
		}
	}
	if len(matchingRows) == 0 {
		return nil
	}

	col := child.Cols[ref.childColIdx]
	if col.NotNull || col.Constraint == storage.PrimaryKey {
		return fmt.Errorf("FOREIGN KEY constraint violation: SET NULL cannot null NOT NULL column %q on table %q",
			col.Name, child.Name)
	}
	childChanges := make(map[any]fkChange, len(matchingRows))
	for _, rowID := range matchingRows {
		childChanges[comparableKeyPart(child.Rows[rowID][ref.childColIdx])] = fkChange{newVal: nil}
	}
	if err := fkOnUpdateCheckColumn(env, child, ref.childColIdx, childChanges); err != nil {
		return err
	}

	wal, err := beginWALAuto(env, child.Name)
	if err != nil {
		return err
	}
	for _, rowID := range matchingRows {
		before := append([]any(nil), child.Rows[rowID]...)
		after := append([]any(nil), before...)
		after[ref.childColIdx] = nil
		if err := validateRowConstraints(env, child, after, rowID); err != nil {
			return err
		}
		if err := child.CheckSecondaryIndexConstraints(after, rowID); err != nil {
			return err
		}
		patchConstraintIndexRow(child, rowID, before, after)
		child.Rows[rowID] = after
		if err := child.UpdateSecondaryIndexRow(rowID, before, after); err != nil {
			return err
		}
		if err := wal.logUpdate(env, rowID, before, after, child.Cols); err != nil {
			return err
		}
	}
	if err := wal.commit(); err != nil {
		return err
	}
	child.Version++
	child.InvalidateStats()
	child.MarkDirtyFrom(-1)
	markDependentMaterializedViewsStale(env, child.Name)
	return nil
}

// cascadeForeignKeyReference deletes (for fkChange.deleted) or updates (for
// fkChange.newVal) every matching child row directly, then recurses into
// fkOnDeleteCheckAllColumns for any rows it cascades to delete, so a chain of
// foreign keys (grandchild referencing child referencing parent) cascades
// correctly instead of only one level deep.
//
// This does not re-enter executeDelete/executeUpdate, so SQL triggers and FTS
// index updates do not fire for cascaded rows — a deliberate scope limit; see
// README's foreign key section. The underlying row/index/WAL bookkeeping is
// nevertheless kept identical to normal DML.
func cascadeForeignKeyReference(env ExecEnv, ref fkReference, changes map[any]fkChange) error {
	child := ref.childTable
	var toRemove []int
	type rowUpdate struct {
		rowID  int
		newVal any
	}
	updates := make([]rowUpdate, 0)
	for i, r := range child.Rows {
		if ref.childColIdx >= len(r) || r[ref.childColIdx] == nil {
			continue
		}
		ch, ok := changes[comparableKeyPart(r[ref.childColIdx])]
		if !ok {
			continue
		}
		if ch.deleted {
			toRemove = append(toRemove, i)
		} else {
			updates = append(updates, rowUpdate{rowID: i, newVal: ch.newVal})
		}
	}
	if len(toRemove) == 0 && len(updates) == 0 {
		return nil
	}
	for _, update := range updates {
		if isNull(update.newVal) && (child.Cols[ref.childColIdx].NotNull || child.Cols[ref.childColIdx].Constraint == storage.PrimaryKey) {
			return fmt.Errorf("FOREIGN KEY constraint violation: CASCADE cannot assign NULL to NOT NULL column %q on table %q",
				child.Cols[ref.childColIdx].Name, child.Name)
		}
	}
	if len(updates) > 0 {
		childChanges := make(map[any]fkChange, len(updates))
		for _, update := range updates {
			childChanges[comparableKeyPart(child.Rows[update.rowID][ref.childColIdx])] = fkChange{newVal: update.newVal}
		}
		if err := fkOnUpdateCheckColumn(env, child, ref.childColIdx, childChanges); err != nil {
			return err
		}
	}

	deletedRaw := make([][]any, len(toRemove))
	for j, idx := range toRemove {
		deletedRaw[j] = child.Rows[idx]
	}
	if err := fkOnDeleteCheckAllColumns(env, child, deletedRaw); err != nil {
		return err
	}

	wal, err := beginWALAuto(env, child.Name)
	if err != nil {
		return err
	}
	for _, update := range updates {
		before := append([]any(nil), child.Rows[update.rowID]...)
		after := append([]any(nil), before...)
		after[ref.childColIdx] = update.newVal
		// The parent row is deliberately updated after this referential action,
		// so validating this one foreign-key column against the current parent
		// state would incorrectly reject a valid ON UPDATE CASCADE. All other
		// constraints are unchanged; materialized unique indexes still need the
		// usual duplicate check.
		if err := child.CheckSecondaryIndexConstraints(after, update.rowID); err != nil {
			return err
		}
		patchConstraintIndexRow(child, update.rowID, before, after)
		child.Rows[update.rowID] = after
		if err := child.UpdateSecondaryIndexRow(update.rowID, before, after); err != nil {
			return err
		}
		if err := wal.logUpdate(env, update.rowID, before, after, child.Cols); err != nil {
			return err
		}
	}

	if len(toRemove) > 0 {
		removeSet := make(map[int]bool, len(toRemove))
		for _, idx := range toRemove {
			removeSet[idx] = true
		}
		kept := make([][]any, 0, len(child.Rows)-len(toRemove))
		oldToNew := make(map[int]int, len(child.Rows)-len(toRemove))
		for i, r := range child.Rows {
			if removeSet[i] {
				if err := wal.logDelete(env, i, r, child.Cols); err != nil {
					return err
				}
				continue
			}
			oldToNew[i] = len(kept)
			kept = append(kept, r)
		}
		child.Rows = kept
		child.ReindexSecondaryIndexRows(oldToNew)
		invalidateConstraintIndexes(child)
	}
	if err := wal.commit(); err != nil {
		return err
	}
	child.Version++
	child.InvalidateStats()
	child.MarkDirtyFrom(-1)
	markDependentMaterializedViewsStale(env, child.Name)
	return nil
}

// checkForeignKeysBeforeDelete is called once at the top of executeDelete,
// before any of its three internal paths (delete-all, raw fast path,
// trigger-aware slow path) runs. It is a read-only pre-pass: it evaluates
// the same WHERE clause those paths will evaluate anyway, purely to learn
// which rows would be removed, then hands them to
// fkOnDeleteCheckAllColumns. Structuring it this way — rather than teaching
// each of executeDelete's three paths about foreign keys individually —
// keeps this a single, easy-to-audit choke point, at the cost of
// evaluating WHERE twice when (and only when) the tenant has any foreign
// keys defined at all (tenantHasAnyForeignKeys short-circuits the common
// case where none exist).
func checkForeignKeysBeforeDelete(env ExecEnv, t *storage.Table, where Expr) error {
	if !tenantHasAnyForeignKeys(env) {
		return nil
	}
	var matched [][]any
	if where == nil {
		matched = t.Rows
	} else {
		tablePrefix := strings.ToLower(t.Name) + "."
		for _, r := range t.Rows {
			row := buildTableRow(t.Cols, tablePrefix, r)
			v, err := evalExpr(env, where, row)
			if err != nil {
				return err
			}
			if toTri(v) == tvTrue {
				matched = append(matched, r)
			}
		}
	}
	return fkOnDeleteCheckAllColumns(env, t, matched)
}

// checkForeignKeysBeforeUpdate is executeUpdate's equivalent of
// checkForeignKeysBeforeDelete: a read-only pre-pass, skipped entirely when
// the tenant has no foreign keys, that finds which SET clauses would
// actually change the value of a column some other table's foreign key
// references, and enforces RESTRICT/CASCADE/SET NULL for those specific
// value changes before the real update runs.
func checkForeignKeysBeforeUpdate(env ExecEnv, t *storage.Table, s *Update) error {
	if !tenantHasAnyForeignKeys(env) {
		return nil
	}

	// Only SET clauses touching a column that's actually referenced by some
	// other table's foreign key are worth tracking.
	setIdx := map[int]Expr{}
	for name, ex := range s.Sets {
		i, err := t.ColIndex(name)
		if err != nil {
			return err
		}
		if len(findReferencingForeignKeys(env, t.Name, t.Cols[i].Name, true)) > 0 ||
			len(findReferencingForeignKeys(env, t.Name, t.Cols[i].Name, false)) > 0 {
			setIdx[i] = ex
		}
	}
	if len(setIdx) == 0 {
		return nil
	}

	tablePrefix := strings.ToLower(t.Name) + "."
	changesByCol := make(map[int]map[any]fkChange, len(setIdx))
	for _, r := range t.Rows {
		row := buildTableRow(t.Cols, tablePrefix, r)
		match := s.Where == nil
		if !match {
			v, err := evalExpr(env, s.Where, row)
			if err != nil {
				return err
			}
			match = toTri(v) == tvTrue
		}
		if !match {
			continue
		}
		for colIdx, ex := range setIdx {
			oldVal := r[colIdx]
			if oldVal == nil {
				continue // nothing referenced a NULL value to begin with
			}
			newVal, err := evalExpr(env, ex, row)
			if err != nil {
				return err
			}
			cv, err := coerceColumnValue(newVal, t.Cols[colIdx])
			if err != nil {
				return err
			}
			if rawEqual(oldVal, cv) {
				continue // SET assigns the same value back; nothing actually changes
			}
			if changesByCol[colIdx] == nil {
				changesByCol[colIdx] = map[any]fkChange{}
			}
			changesByCol[colIdx][comparableKeyPart(oldVal)] = fkChange{newVal: cv}
		}
	}
	for colIdx, changes := range changesByCol {
		if err := fkOnUpdateCheckColumn(env, t, colIdx, changes); err != nil {
			return err
		}
	}
	return nil
}
