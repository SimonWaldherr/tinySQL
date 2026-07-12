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
	switch ref.action {
	case storage.Cascade:
		return cascadeForeignKeyReference(env, ref, changes)
	case storage.SetNull:
		return setNullForeignKeyReference(ref, changes)
	default: // Restrict, NoAction
		return restrictForeignKeyReference(ref, changes)
	}
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
// matching child row, for both deletions and updates.
func setNullForeignKeyReference(ref fkReference, changes map[any]fkChange) error {
	child := ref.childTable
	touched := false
	for i, r := range child.Rows {
		if ref.childColIdx >= len(r) || r[ref.childColIdx] == nil {
			continue
		}
		if _, ok := changes[comparableKeyPart(r[ref.childColIdx])]; ok {
			child.Rows[i][ref.childColIdx] = nil
			touched = true
		}
	}
	if touched {
		child.Version++
		invalidateConstraintIndexes(child)
	}
	return nil
}

// cascadeForeignKeyReference deletes (for fkChange.deleted) or updates (for
// fkChange.newVal) every matching child row directly, then recurses into
// fkOnDeleteCheckAllColumns for any rows it cascades to delete, so a chain of
// foreign keys (grandchild referencing child referencing parent) cascades
// correctly instead of only one level deep.
//
// This mutates child.Rows directly rather than re-entering
// executeDelete/executeUpdate, so triggers and FTS index updates do not fire
// for cascaded rows — a deliberate scope limit; see README's foreign key
// section.
func cascadeForeignKeyReference(env ExecEnv, ref fkReference, changes map[any]fkChange) error {
	child := ref.childTable
	var toRemove []int
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
			child.Rows[i][ref.childColIdx] = ch.newVal
		}
	}
	if len(toRemove) == 0 {
		child.Version++
		invalidateConstraintIndexes(child)
		return nil
	}

	deletedRaw := make([][]any, len(toRemove))
	for j, idx := range toRemove {
		deletedRaw[j] = child.Rows[idx]
	}
	if err := fkOnDeleteCheckAllColumns(env, child, deletedRaw); err != nil {
		return err
	}

	removeSet := make(map[int]bool, len(toRemove))
	for _, idx := range toRemove {
		removeSet[idx] = true
	}
	kept := make([][]any, 0, len(child.Rows)-len(toRemove))
	for i, r := range child.Rows {
		if !removeSet[i] {
			kept = append(kept, r)
		}
	}
	child.Rows = kept
	child.Version++
	invalidateConstraintIndexes(child)
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
