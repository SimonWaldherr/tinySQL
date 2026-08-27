package engine

import (
	"context"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestNoOpPointUpdateCountsMatchWithoutDirtyingTable(t *testing.T) {
	db := storage.NewDB()
	ctx := context.Background()
	for _, sql := range []string{
		`CREATE TABLE items (id INT PRIMARY KEY, bucket INT, value TEXT)`,
		`INSERT INTO items VALUES (1, 7, 'same')`,
		`CREATE INDEX idx_items_bucket ON items(bucket)`,
		`ANALYZE items`,
	} {
		if _, err := Execute(ctx, db, "default", mustParse(sql)); err != nil {
			t.Fatalf("execute %q: %v", sql, err)
		}
	}
	table, err := db.Get("default", "items")
	if err != nil {
		t.Fatal(err)
	}
	version := table.Version
	table.ResetDirty()
	dirtyFrom := table.DirtyFrom()

	rs, err := Execute(ctx, db, "default", mustParse(`UPDATE items SET value = 'same' WHERE id = 1`))
	if err != nil {
		t.Fatal(err)
	}
	if got := expectAsInt(t, rs.Rows[0]["updated"]); got != 1 {
		t.Fatalf("matched rows = %d, want 1", got)
	}
	if table.Version != version {
		t.Fatalf("no-op UPDATE changed table version from %d to %d", version, table.Version)
	}
	if stats := table.Statistics(); stats == nil || stats.Stale {
		t.Fatalf("no-op UPDATE invalidated statistics: %#v", stats)
	}
	if rows, _ := table.DirtyRows(); table.DirtyFrom() != dirtyFrom || len(rows) != 0 {
		t.Fatalf("no-op UPDATE changed dirty tracking: dirtyFrom=%d (before %d), rows=%#v", table.DirtyFrom(), dirtyFrom, rows)
	}
	index := table.FindSecondaryIndex([]string{"bucket"})
	rowIDs, err := table.LookupSecondaryIndexPoint(index, []any{table.Rows[0][1]})
	if err != nil || len(rowIDs) != 1 || rowIDs[0] != 0 {
		t.Fatalf("secondary index after no-op UPDATE = %#v, %v", rowIDs, err)
	}
}

func TestFailedBoundedIndexedUpdateRestoresOldIndexKey(t *testing.T) {
	db := storage.NewDB()
	ctx := context.Background()
	for _, sql := range []string{
		`CREATE TABLE items (id INT PRIMARY KEY, bucket INT UNIQUE, value INT)`,
		`INSERT INTO items VALUES (1, 10, 1), (2, 20, 2)`,
		`CREATE INDEX idx_items_bucket ON items(bucket)`,
	} {
		if _, err := Execute(ctx, db, "default", mustParse(sql)); err != nil {
			t.Fatalf("execute %q: %v", sql, err)
		}
	}

	table, getErr := db.Get("default", "items")
	if getErr != nil {
		t.Fatal(getErr)
	}
	update := mustParse(`UPDATE items SET bucket = 30 WHERE id = 1`).(*Update)
	_, rowIDs, ok := rowUpdateSnapshotTarget(newDMLPlan(&dmlPlan{}, db, "default", update))
	if !ok || len(rowIDs) != 1 || rowIDs[0] != 0 {
		t.Fatalf("indexed UPDATE did not select bounded rollback: %#v, %v", rowIDs, ok)
	}

	db.LockContentForWrite()
	snapshot, err := db.SnapshotForRowUpdateStatement("default", "items", rowIDs)
	if err != nil {
		db.UnlockContentForWrite()
		t.Fatal(err)
	}
	before := table.Rows[0]
	after := append([]any(nil), before...)
	after[1], err = coerceColumnValue(float64(30), table.Cols[1])
	if err != nil {
		db.UnlockContentForWrite()
		t.Fatal(err)
	}
	table.Rows[0] = after
	if err := table.UpdateSecondaryIndexRow(0, before, after, table.SortedIndexNames()); err != nil {
		db.UnlockContentForWrite()
		t.Fatal(err)
	}
	db.RestoreStatementSnapshot(snapshot)
	db.ReleaseStatementSnapshot(snapshot)
	db.UnlockContentForWrite()

	index := table.FindSecondaryIndex([]string{"bucket"})
	for _, wantRow := range []int{0, 1} {
		key := table.Rows[wantRow][1]
		rowIDs, lookupErr := table.LookupSecondaryIndexPoint(index, []any{key})
		if lookupErr != nil || len(rowIDs) != 1 || rowIDs[0] != wantRow {
			t.Fatalf("restored index key %v = %#v, %v; want [%d]", key, rowIDs, lookupErr, wantRow)
		}
	}
}
