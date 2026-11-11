package engine

import (
	"reflect"
	"testing"
)

func TestConstraintBorrowRefusalPreservesLenderState(t *testing.T) {
	for _, collapsed := range []bool{false, true} {
		unique := newConstraintIndexEntry(0)
		unique.add("unique", 1)
		alreadyLent := newConstraintIndexEntry(0)
		alreadyLent.add("held", 2)
		priorBorrower, ok := alreadyLent.borrow()
		if !ok {
			t.Fatal("unique entry refused its first borrower")
		}
		multi := newConstraintIndexEntry(0)
		multi.add("duplicate", 3)
		multi.add("duplicate", 4)
		if collapsed {
			multi.remove("duplicate", 4)
		}
		set := &constraintIndexSet{cols: map[int]*constraintIndexEntry{0: unique, 1: alreadyLent, 2: multi}}
		before := map[int]map[any][]int{}
		lent := map[int]bool{}
		for col, entry := range set.cols {
			before[col], lent[col] = entry.buckets(), entry.lent
		}
		// Refusal is an eligibility check, including for a historical bucket
		// that has collapsed. It must allocate no destination state.
		allocations := testing.AllocsPerRun(100, func() {
			if borrowed, ok := set.BorrowDerived(); ok || borrowed != nil {
				t.Fatal("borrow accepted a set with multi-bucket state")
			}
		})
		if allocations != 0 {
			t.Fatalf("collapsed=%v: refusal allocated %v objects", collapsed, allocations)
		}
		for col, entry := range set.cols {
			if entry.lent != lent[col] || !reflect.DeepEqual(entry.buckets(), before[col]) {
				t.Fatalf("collapsed=%v: failed borrow changed column %d", collapsed, col)
			}
		}
		// Failure must neither create a phantom lender nor undo a real one.
		originalMap := unique.rows
		unique.add("new", 5)
		if originalMap["new"] != 5 {
			t.Fatal("failed borrow forced a unique entry to copy its map")
		}
		alreadyLent.add("later", 6)
		if priorBorrower.exists("later", -1) {
			t.Fatal("failed borrow removed protection for an existing borrower")
		}
	}
}

func TestConstraintBorrowPreflightKeepsIndependentColumns(t *testing.T) {
	for _, columns := range []map[int]*constraintIndexEntry{nil, {}} {
		value, ok := (&constraintIndexSet{cols: columns}).BorrowDerived()
		if !ok || len(value.(*constraintIndexSet).cols) != 0 {
			t.Fatal("empty constraint set could not be borrowed")
		}
	}
	set := &constraintIndexSet{cols: make(map[int]*constraintIndexEntry)}
	for col := range 4 {
		entry := newConstraintIndexEntry(0)
		entry.add(nil, col)
		for key := range 100 {
			entry.add(key, key+1)
		}
		// Carry a real lower layer and enough changes to lend a middle layer.
		entry, _ = entry.borrow()
		for key := 100; key < 100+constraintOverlayCarry+1; key++ {
			entry.add(key, key+1)
		}
		set.cols[col] = entry
	}
	firstValue, ok := set.BorrowDerived()
	if !ok {
		t.Fatal("unique columns refused borrowing")
	}
	first := firstValue.(*constraintIndexSet)
	secondValue, ok := first.BorrowDerived()
	if !ok {
		t.Fatal("borrowed columns refused another borrower")
	}
	second := secondValue.(*constraintIndexSet)
	for col := range 4 {
		first.cols[col].remove(nil, col)
		second.cols[col].add("second", col)
		set.cols[col].add("source", col)
		if !set.cols[col].exists(nil, -1) || !second.cols[col].exists(nil, -1) || first.cols[col].exists(nil, -1) {
			t.Fatalf("column %d: removing a NULL key leaked to another entry", col)
		}
		if first.cols[col].exists("source", -1) || second.cols[col].exists("source", -1) || set.cols[col].exists("second", -1) {
			t.Fatalf("column %d: descendant or source mutations leaked", col)
		}
	}
	first.cols[99] = newConstraintIndexEntry(0)
	if set.cols[99] != nil || second.cols[99] != nil {
		t.Fatal("borrowers share their mutable column directory")
	}
}
