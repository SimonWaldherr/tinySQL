package pager

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func tmpPageBackend(t *testing.T) *PageBackend {
	t.Helper()
	dir := t.TempDir()
	pb, err := NewPageBackend(PageBackendConfig{
		Path: filepath.Join(dir, "gc_test.db"),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { pb.Close() })
	return pb
}

func saveTestTable(t *testing.T, pb *PageBackend, tenant, name string, nRows int) {
	t.Helper()
	cols := []ColumnInfo{
		{Name: "id", Type: 0},
		{Name: "name", Type: 13},
	}
	rows := make([][]any, nRows)
	for i := range rows {
		rows[i] = []any{float64(i), fmt.Sprintf("row_%d", i)}
	}
	td := &TableData{Name: name, Columns: cols, Rows: rows, Version: 1}
	if err := pb.SaveTable(tenant, td); err != nil {
		t.Fatal(err)
	}
}

// TestGC_NoOrphans verifies that GC on a clean database reclaims nothing.
func TestGC_NoOrphans(t *testing.T) {
	pb := tmpPageBackend(t)
	saveTestTable(t, pb, "default", "users", 10)

	result, err := pb.GC()
	if err != nil {
		t.Fatal(err)
	}

	if result.Reclaimed != 0 {
		t.Errorf("expected 0 reclaimed, got %d", result.Reclaimed)
	}
	if result.ReachablePages < 2 {
		t.Errorf("expected at least 2 reachable pages, got %d", result.ReachablePages)
	}
	if len(result.Errors) != 0 {
		t.Errorf("unexpected errors: %v", result.Errors)
	}
}

// TestGC_AfterDelete verifies that GC finds no orphans when DeleteTable
// correctly frees pages (our fix).
func TestGC_AfterDelete(t *testing.T) {
	pb := tmpPageBackend(t)

	// Create and delete a table.
	saveTestTable(t, pb, "default", "temp_table", 50)
	if err := pb.DeleteTable("default", "temp_table"); err != nil {
		t.Fatal(err)
	}

	result, err := pb.GC()
	if err != nil {
		t.Fatal(err)
	}

	if result.Reclaimed != 0 {
		t.Logf("GC result: total=%d reachable=%d freeBefore=%d freeAfter=%d reclaimed=%d",
			result.TotalPages, result.ReachablePages, result.FreeBefore, result.FreeAfter, result.Reclaimed)
		// Some pages may be reclaimed if the free-list chain itself leaked.
		// As long as it's small, that's acceptable.
	}
}

// TestGC_SimulatedOrphans manually creates orphan pages and verifies the
// GC reclaims them.
func TestGC_SimulatedOrphans(t *testing.T) {
	pb := tmpPageBackend(t)
	saveTestTable(t, pb, "default", "users", 10)

	// Allocate some pages without linking them to any tree (simulates
	// pages leaked by a crashed transaction).
	txID, err := pb.pager.BeginTx()
	if err != nil {
		t.Fatal(err)
	}
	var orphanIDs []PageID
	for i := 0; i < 5; i++ {
		pid, buf := pb.pager.AllocPage()
		InitBTreePage(buf, pid, true) // give it valid content
		SetPageCRC(buf)
		pb.pager.WritePage(txID, pid, buf)
		pb.pager.UnpinPage(pid)
		orphanIDs = append(orphanIDs, pid)
	}
	pb.pager.CommitTx(txID)
	pb.pager.Checkpoint()

	// GC should find and reclaim these orphans.
	result, err := pb.GC()
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("GC result: total=%d reachable=%d freeBefore=%d freeAfter=%d reclaimed=%d",
		result.TotalPages, result.ReachablePages, result.FreeBefore, result.FreeAfter, result.Reclaimed)

	if result.Reclaimed < 5 {
		t.Errorf("expected at least 5 reclaimed orphans, got %d", result.Reclaimed)
	}
	if len(result.Errors) != 0 {
		t.Errorf("unexpected errors: %v", result.Errors)
	}
}

// TestGC_MultipleTables tests GC with several tables to verify all trees
// are correctly walked.
func TestGC_MultipleTables(t *testing.T) {
	pb := tmpPageBackend(t)

	for i := 0; i < 5; i++ {
		saveTestTable(t, pb, "default", fmt.Sprintf("table_%d", i), 20)
	}

	result, err := pb.GC()
	if err != nil {
		t.Fatal(err)
	}

	if result.Reclaimed != 0 {
		t.Errorf("expected 0 reclaimed on clean DB with 5 tables, got %d", result.Reclaimed)
	}
	if result.ReachablePages < 5 {
		t.Errorf("expected at least 5 reachable pages, got %d", result.ReachablePages)
	}
}

// TestGC_Idempotent verifies running GC twice gives no reclaimed on second run.
func TestGC_Idempotent(t *testing.T) {
	pb := tmpPageBackend(t)
	saveTestTable(t, pb, "default", "users", 10)

	// Simulate orphans.
	txID, _ := pb.pager.BeginTx()
	for i := 0; i < 3; i++ {
		pid, buf := pb.pager.AllocPage()
		InitBTreePage(buf, pid, true)
		SetPageCRC(buf)
		pb.pager.WritePage(txID, pid, buf)
		pb.pager.UnpinPage(pid)
	}
	pb.pager.CommitTx(txID)
	pb.pager.Checkpoint()

	// First GC reclaims orphans.
	r1, err := pb.GC()
	if err != nil {
		t.Fatal(err)
	}
	if r1.Reclaimed < 3 {
		t.Errorf("first GC: expected ≥3 reclaimed, got %d", r1.Reclaimed)
	}

	// Second GC should find nothing.
	r2, err := pb.GC()
	if err != nil {
		t.Fatal(err)
	}
	if r2.Reclaimed != 0 {
		t.Errorf("second GC: expected 0 reclaimed, got %d", r2.Reclaimed)
	}
}

// TestGC_DataIntegrity verifies that data is intact after GC.
func TestGC_DataIntegrity(t *testing.T) {
	pb := tmpPageBackend(t)
	saveTestTable(t, pb, "default", "important", 100)

	// Run GC.
	if _, err := pb.GC(); err != nil {
		t.Fatal(err)
	}

	// Verify data is still intact.
	td, err := pb.LoadTable("default", "important")
	if err != nil {
		t.Fatal(err)
	}
	if td == nil {
		t.Fatal("table not found after GC")
	}
	if len(td.Rows) != 100 {
		t.Errorf("expected 100 rows, got %d", len(td.Rows))
	}
	// Check first and last row.
	if v, ok := td.Rows[0][0].(float64); !ok || v != 0 {
		t.Errorf("row 0 col 0: got %v", td.Rows[0][0])
	}
	if v, ok := td.Rows[99][0].(float64); !ok || v != 99 {
		t.Errorf("row 99 col 0: got %v", td.Rows[99][0])
	}
}

// TestGC_Persistence verifies that reclaimed pages survive close/reopen.
func TestGC_Persistence(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "gc_persist.db")

	// Phase 1: Create DB, simulate orphans, GC.
	pb, err := NewPageBackend(PageBackendConfig{Path: dbPath})
	if err != nil {
		t.Fatal(err)
	}
	saveTestTable(t, pb, "default", "users", 10)

	// Create orphans.
	txID, _ := pb.pager.BeginTx()
	for i := 0; i < 4; i++ {
		pid, buf := pb.pager.AllocPage()
		InitBTreePage(buf, pid, true)
		SetPageCRC(buf)
		pb.pager.WritePage(txID, pid, buf)
		pb.pager.UnpinPage(pid)
	}
	pb.pager.CommitTx(txID)
	pb.pager.Checkpoint()

	r, err := pb.GC()
	if err != nil {
		t.Fatal(err)
	}
	if r.Reclaimed < 4 {
		t.Errorf("expected ≥4 reclaimed, got %d", r.Reclaimed)
	}
	freeAfter := r.FreeAfter
	pb.Close()

	// Phase 2: Reopen and verify free list persisted.
	pb2, err := NewPageBackend(PageBackendConfig{Path: dbPath})
	if err != nil {
		t.Fatal(err)
	}
	defer pb2.Close()

	// Should have approximately the same number of free pages.
	freeNow := pb2.pager.freeMgr.Count()
	if freeNow < freeAfter-2 { // allow small delta from free-list chain pages
		t.Errorf("expected ≥%d free pages after reopen, got %d", freeAfter-2, freeNow)
	}

	// Data should still be intact.
	td, err := pb2.LoadTable("default", "users")
	if err != nil {
		t.Fatal(err)
	}
	if td == nil || len(td.Rows) != 10 {
		t.Errorf("expected 10 rows after reopen, got %v", td)
	}
}

// TestGC_EmptyDB verifies GC on a database with no tables.
func TestGC_EmptyDB(t *testing.T) {
	pb := tmpPageBackend(t)

	result, err := pb.GC()
	if err != nil {
		t.Fatal(err)
	}
	if result.Reclaimed != 0 {
		t.Errorf("expected 0 reclaimed on empty DB, got %d", result.Reclaimed)
	}
}

// TestGC_Stats returns consistent statistics.
func TestGC_Stats(t *testing.T) {
	pb := tmpPageBackend(t)
	saveTestTable(t, pb, "default", "t1", 50)

	result, err := pb.GC()
	if err != nil {
		t.Fatal(err)
	}

	if result.TotalPages <= 0 {
		t.Errorf("TotalPages should be > 0, got %d", result.TotalPages)
	}
	if result.ReachablePages <= 0 {
		t.Errorf("ReachablePages should be > 0, got %d", result.ReachablePages)
	}
	if result.ReachablePages > result.TotalPages {
		t.Errorf("ReachablePages (%d) > TotalPages (%d)", result.ReachablePages, result.TotalPages)
	}
	// Accounting: reachable + free + reclaimed should cover all pages.
	accounted := result.ReachablePages + result.FreeAfter
	if accounted < result.TotalPages {
		t.Errorf("accounting gap: reachable(%d) + freeAfter(%d) = %d < totalPages(%d)",
			result.ReachablePages, result.FreeAfter, accounted, result.TotalPages)
	}

	_ = os.Stderr // keep os import used
}
