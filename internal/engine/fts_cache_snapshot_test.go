package engine

import (
	"context"
	"fmt"
	"sync"
	"testing"
)

// getFTSDocCache and prepareFTSQuery serve warm hits from lock-free
// atomic.Pointer snapshots instead of taking their RWMutex read locks. The
// snapshots are refreshed under the same mutex the authoritative maps are
// mutated under, so a reader can never see a snapshot older than what the map
// already reflects.
//
// Worth being precise about what each test below can and cannot catch, because
// the two properties have different failure modes:
//
//   - Correctness does NOT depend on a mutation site republishing. Every entry
//     carries the *storage.Table pointer and Version it was built from, and both
//     the snapshot path and the locked path reject an entry that fails that
//     comparison. A stale snapshot entry is therefore ignored, not served, which
//     is why TestFTSDocCacheSnapshotFollowsInvalidation still passes if a
//     republish is deleted — verified by deleting one. It guards the
//     invalidation contract end to end, not the republish specifically.
//   - Memory reclamation DOES depend on it: a purge that updated only the
//     authoritative map would leave the snapshot holding the dropped table and
//     every row reachable through it, which is the leak purgeFTSCachesFor exists
//     to prevent. TestFTSDocCacheSnapshotReleasesDroppedTable covers that, and
//     does fail if the republish in purgeFTSCachesFor is removed.

// TestFTSDocCacheSnapshotFollowsInvalidation pins the sequential invariant:
// after rows are inserted or the table is dropped and rebuilt, a search must
// reflect the new corpus rather than a snapshot of the old one.
func TestFTSDocCacheSnapshotFollowsInvalidation(t *testing.T) {
	db := setupTestDB()
	execSQL(t, db, `CREATE TABLE snap_docs (id INT, body TEXT)`)
	execSQL(t, db, `INSERT INTO snap_docs VALUES (1, 'alpha document')`)

	countHits := func(query string) int {
		rs := execSQL(t, db, fmt.Sprintf(
			`SELECT id FROM FTS_SEARCH('snap_docs', '%s', 50, 'body')`, query))
		return len(rs.Rows)
	}

	if n := countHits("alpha"); n != 1 {
		t.Fatalf("initial search for alpha = %d rows, want 1", n)
	}
	if n := countHits("beta"); n != 0 {
		t.Fatalf("search for not-yet-inserted beta = %d rows, want 0", n)
	}

	// An INSERT bumps table.Version, which must invalidate both the tokenized
	// document snapshot and the prepared plan snapshot for the same query text.
	execSQL(t, db, `INSERT INTO snap_docs VALUES (2, 'beta document')`)
	if n := countHits("beta"); n != 1 {
		t.Errorf("after INSERT, search for beta = %d rows, want 1 (stale doc-cache snapshot?)", n)
	}
	if n := countHits("alpha"); n != 1 {
		t.Errorf("after INSERT, search for alpha = %d rows, want 1", n)
	}
	if n := countHits("document"); n != 2 {
		t.Errorf("after INSERT, search for document = %d rows, want 2", n)
	}

	// DROP purges both caches eagerly; the purge must republish, or the
	// recreated table's search would still be answered from the dropped
	// table's corpus.
	execSQL(t, db, `DROP TABLE snap_docs`)
	execSQL(t, db, `CREATE TABLE snap_docs (id INT, body TEXT)`)
	execSQL(t, db, `INSERT INTO snap_docs VALUES (9, 'gamma document')`)
	if n := countHits("alpha"); n != 0 {
		t.Errorf("after DROP and recreate, search for alpha = %d rows, want 0 (purge did not republish?)", n)
	}
	if n := countHits("gamma"); n != 1 {
		t.Errorf("after DROP and recreate, search for gamma = %d rows, want 1", n)
	}
}

// TestFTSDocCacheSnapshotReleasesDroppedTable checks the memory-reclamation
// half of the contract: after DROP TABLE, the lock-free snapshot must not still
// hold the dropped table's tokenized corpus. Correctness would survive that (the
// entry's table pointer no longer matches, so it is never served), but the
// dropped table and all its rows would stay reachable through the snapshot for
// as long as the process ran without another FTS write — exactly the leak
// purgeFTSCachesFor exists to prevent.
func TestFTSDocCacheSnapshotReleasesDroppedTable(t *testing.T) {
	db := setupTestDB()
	execSQL(t, db, `CREATE TABLE dropped_docs (id INT, body TEXT)`)
	execSQL(t, db, `INSERT INTO dropped_docs VALUES (1, 'alpha document')`)
	execSQL(t, db, `SELECT id FROM FTS_SEARCH('dropped_docs', 'alpha', 10, 'body')`)

	snapshotHolds := func() bool {
		snap := ftsDocCacheSnapshot.Load()
		if snap == nil {
			return false
		}
		for k := range *snap {
			if k.table == "dropped_docs" {
				return true
			}
		}
		return false
	}

	if !snapshotHolds() {
		t.Fatal("expected the warm search to publish a snapshot entry for dropped_docs")
	}
	execSQL(t, db, `DROP TABLE dropped_docs`)
	if snapshotHolds() {
		t.Error("snapshot still holds the dropped table's corpus; purgeFTSCachesFor must republish")
	}
}

// TestFTSSearchConcurrentReadersAndWriters races many concurrent FTS_SEARCH
// readers — the calls that take the lock-free fast paths — against writers that
// keep bumping the table version and so keep invalidating both snapshots.
//
// Without -race in this environment (cgo is unavailable), this still catches the
// outward symptoms a broken snapshot would produce: a panic, or a search that
// misses a row the corpus definitely contains.
func TestFTSSearchConcurrentReadersAndWriters(t *testing.T) {
	db := setupTestDB()
	execSQL(t, db, `CREATE TABLE concurrent_docs (id INT, body TEXT)`)
	// Every row contains "shared", so a search for it must never return zero
	// rows no matter which corpus version a reader happens to observe.
	for i := 0; i < 20; i++ {
		execSQL(t, db, fmt.Sprintf(
			`INSERT INTO concurrent_docs VALUES (%d, 'shared token filler%d')`, i, i))
	}

	const (
		readers      = 12
		readsEach    = 40
		writerInsert = 25
	)

	var wg sync.WaitGroup
	errs := make(chan error, readers+1)

	wg.Add(readers)
	for r := 0; r < readers; r++ {
		go func() {
			defer wg.Done()
			for i := 0; i < readsEach; i++ {
				rs, err := Execute(context.Background(), db, "default",
					mustParse(`SELECT id FROM FTS_SEARCH('concurrent_docs', 'shared', 100, 'body')`))
				if err != nil {
					errs <- fmt.Errorf("concurrent FTS_SEARCH failed: %w", err)
					return
				}
				if len(rs.Rows) == 0 {
					errs <- fmt.Errorf("FTS_SEARCH for 'shared' returned 0 rows; every row contains it")
					return
				}
			}
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < writerInsert; i++ {
			if _, err := Execute(context.Background(), db, "default", mustParse(fmt.Sprintf(
				`INSERT INTO concurrent_docs VALUES (%d, 'shared later%d')`, 1000+i, i))); err != nil {
				errs <- fmt.Errorf("concurrent INSERT failed: %w", err)
				return
			}
		}
	}()

	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}

	// After all writes settle, the corpus must be fully visible — the final
	// snapshot has to reflect every committed insert.
	rs := execSQL(t, db, `SELECT id FROM FTS_SEARCH('concurrent_docs', 'shared', 200, 'body')`)
	if want := 20 + writerInsert; len(rs.Rows) != want {
		t.Errorf("final FTS_SEARCH returned %d rows, want %d (snapshot missed committed writes?)",
			len(rs.Rows), want)
	}
}
