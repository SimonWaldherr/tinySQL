package storage

import (
	"testing"
)

func newSharedTestDB(t *testing.T) (*DB, *Table, *Table) {
	t.Helper()
	db := NewDB()
	big := NewTable("big", []Column{{Name: "id", Type: IntType}, {Name: "v", Type: TextType}}, false)
	small := NewTable("small", []Column{{Name: "id", Type: IntType}}, false)
	for i := 0; i < 50; i++ {
		big.Rows = append(big.Rows, []any{i, "row"})
	}
	small.Rows = append(small.Rows, []any{1})
	big.Version, small.Version = 3, 2
	if err := db.Put("default", big); err != nil {
		t.Fatal(err)
	}
	if err := db.Put("default", small); err != nil {
		t.Fatal(err)
	}
	return db, big, small
}

func takeShared(t *testing.T, db *DB) (base, shadow *DB, release func()) {
	t.Helper()
	db.LockContentForRead()
	defer db.UnlockContentForRead()
	return db.SnapshotForTxShared()
}

// Building the snapshot copies no table: the shadow and the live database hold
// the very same *Table values, and both sides see them pinned.
func TestSnapshotForTxSharedCopiesNothing(t *testing.T) {
	db, big, small := newSharedTestDB(t)
	_, shadow, release := takeShared(t, db)
	defer release()

	for name, live := range map[string]*Table{"big": big, "small": small} {
		got, err := shadow.Get("default", name)
		if err != nil {
			t.Fatal(err)
		}
		if got != live {
			t.Fatalf("shadow table %s was copied at BEGIN", name)
		}
		if !db.IsTablePinnedForStream(live) {
			t.Fatalf("live table %s is not pinned for the transaction", name)
		}
		if !shadow.IsTablePinnedForStream(got) {
			t.Fatalf("shadow does not treat shared table %s as pinned", name)
		}
	}
}

// The shadow's first write to a table gives it a private copy; the live table is
// left exactly as it was, and an untouched table stays shared.
func TestSharedShadowWriteTakesPrivateCopy(t *testing.T) {
	db, big, small := newSharedTestDB(t)
	_, shadow, release := takeShared(t, db)
	defer release()

	if !shadow.DetachPinnedTableForWrite("default", "big") {
		t.Fatal("first write to a shared table did not detach it")
	}
	private, _ := shadow.Get("default", "big")
	if private == big {
		t.Fatal("shadow still points at the live table after detaching")
	}
	private.Rows = append(private.Rows, []any{99, "tx"})
	private.Rows[0] = []any{0, "changed"}
	if len(big.Rows) != 50 || big.Rows[0][1] != "row" {
		t.Fatalf("live table changed through the shadow: %v", big.Rows[0])
	}
	if db.IsTablePinnedForStream(big) {
		t.Fatal("live table stayed pinned after the shadow copied it")
	}

	other, _ := shadow.Get("default", "small")
	if other != small || !db.IsTablePinnedForStream(small) {
		t.Fatal("a table the transaction did not write was copied or unpinned")
	}
	if shadow.DetachPinnedTableForWrite("default", "big") {
		t.Fatal("a table that is already private was detached again")
	}
}

// A live writer that touches a pinned table gets its own copy, so a transaction
// reading the shared instance keeps its BEGIN-time rows.
func TestSharedSnapshotSurvivesLiveWriter(t *testing.T) {
	db, big, _ := newSharedTestDB(t)
	_, shadow, release := takeShared(t, db)
	defer release()

	if !db.DetachPinnedTableForWrite("default", "big") {
		t.Fatal("live writer was not diverted from a table a transaction shares")
	}
	liveNow, _ := db.Get("default", "big")
	if liveNow == big {
		t.Fatal("live database still maps the pinned instance")
	}
	liveNow.Rows = append(liveNow.Rows, []any{100, "later"})
	liveNow.Rows[3] = []any{3, "later"}

	seen, _ := shadow.Get("default", "big")
	if seen != big || len(seen.Rows) != 50 || seen.Rows[3][1] != "row" {
		t.Fatalf("transaction snapshot moved: len=%d row3=%v", len(seen.Rows), seen.Rows[3])
	}
}

// Tables the transaction never wrote are not part of its change set.
func TestCollectWALChangesSkipsSharedTables(t *testing.T) {
	db, _, _ := newSharedTestDB(t)
	base, shadow, release := takeShared(t, db)
	defer release()

	if changes := CollectWALChanges(base, shadow); len(changes) != 0 {
		t.Fatalf("untouched transaction reports %d changes", len(changes))
	}
	shadow.DetachPinnedTableForWrite("default", "big")
	private, _ := shadow.Get("default", "big")
	private.Version++
	changes := CollectWALChanges(base, shadow)
	if len(changes) != 1 || changes[0].Name != "big" || changes[0].Table != private {
		t.Fatalf("changes = %+v, want only the written table", changes)
	}
}

// Releasing the snapshot, as many times as the caller likes, drops every pin
// that is still held and none that is not.
func TestSharedSnapshotReleaseUnpins(t *testing.T) {
	db, big, small := newSharedTestDB(t)
	_, shadow, release := takeShared(t, db)
	shadow.DetachPinnedTableForWrite("default", "big")

	release()
	release()
	if db.IsTablePinnedForStream(big) || db.IsTablePinnedForStream(small) {
		t.Fatal("pins outlived the transaction")
	}
	if len(db.streamSnapshots) != 0 {
		t.Fatalf("live pin table not empty: %v", db.streamSnapshots)
	}
	if shadow.isTxShared(small) {
		t.Fatal("shadow still reports shared tables after release")
	}
}

// A pin another reader holds on the same table survives the transaction's own
// release.
func TestSharedSnapshotReleaseKeepsStreamPins(t *testing.T) {
	db, big, _ := newSharedTestDB(t)
	releaseStream, ok := db.PinTableForStream(big)
	if !ok {
		t.Fatal("stream pin refused")
	}
	_, _, release := takeShared(t, db)
	release()
	if !db.IsTablePinnedForStream(big) {
		t.Fatal("the transaction's release dropped a stream's pin")
	}
	releaseStream()
	if db.IsTablePinnedForStream(big) {
		t.Fatal("stream pin leaked")
	}
}

// DetachAllPinnedForWrite is the conservative fallback for statements whose
// reach is unknown: it must privatize every shared table.
func TestDetachAllPinnedForWriteCopiesEverySharedTable(t *testing.T) {
	db, big, small := newSharedTestDB(t)
	_, shadow, release := takeShared(t, db)
	defer release()

	shadow.DetachAllPinnedForWrite()
	for name, live := range map[string]*Table{"big": big, "small": small} {
		got, _ := shadow.Get("default", name)
		if got == live {
			t.Fatalf("table %s is still shared after DetachAllPinnedForWrite", name)
		}
		if shadow.isTxShared(got) {
			t.Fatalf("table %s still registered as shared", name)
		}
	}
	if db.IsTablePinnedForStream(big) || db.IsTablePinnedForStream(small) {
		t.Fatal("live pins remain after the shadow copied both tables")
	}
}

// A secondary index is lent to the transaction's copy and to the live table's
// copy alike; writing it on either side must not show on the other.
func TestBorrowedSecondaryIndexIsCopyOnWrite(t *testing.T) {
	db, big, _ := newSharedTestDB(t)
	if err := big.CreateSecondaryIndex("big_v", []string{"v"}, false); err != nil {
		t.Fatal(err)
	}
	_, shadow, release := takeShared(t, db)
	defer release()

	shadow.DetachPinnedTableForWrite("default", "big")
	private, _ := shadow.Get("default", "big")
	names := private.SortedIndexNames()
	private.Rows = append(private.Rows, []any{500, "fresh"})
	if err := private.InsertSecondaryIndexRow(len(private.Rows)-1, private.Rows[len(private.Rows)-1], names); err != nil {
		t.Fatal(err)
	}

	idxPrivate := private.Indexes["big_v"]
	idxLive := big.Indexes["big_v"]
	probe := func(idx *SecondaryIndex, tbl *Table, v string) int {
		rows, err := tbl.LookupSecondaryIndexPoint(idx, []any{v})
		if err != nil {
			t.Fatal(err)
		}
		return len(rows)
	}
	if got := probe(idxPrivate, private, "fresh"); got != 1 {
		t.Fatalf("private index lost its own insert: %d", got)
	}
	if got := probe(idxLive, big, "fresh"); got != 0 {
		t.Fatalf("live index saw the transaction's insert: %d", got)
	}
	if got := probe(idxLive, big, "row"); got != 50 {
		t.Fatalf("live index lost rows: %d", got)
	}

	// And the other direction: the live table writes after having lent its list.
	liveNames := big.SortedIndexNames()
	big.Rows = append(big.Rows, []any{600, "liveonly"})
	if err := big.InsertSecondaryIndexRow(len(big.Rows)-1, big.Rows[len(big.Rows)-1], liveNames); err != nil {
		t.Fatal(err)
	}
	if got := probe(idxPrivate, private, "liveonly"); got != 0 {
		t.Fatalf("transaction index saw the live insert: %d", got)
	}
}

// The conflict-detection base points at the live table while the shadow pins
// it, and must hold on to the BEGIN-time Version once the shadow gives the pin
// up: from then on other writers change the live table in place.
func TestSharedBaseKeepsBeginVersionAfterPrivateCopy(t *testing.T) {
	db, big, small := newSharedTestDB(t)
	base, shadow, release := takeShared(t, db)
	defer release()

	if !shadow.DetachPinnedTableForWrite("default", "big") {
		t.Fatal("first write did not detach the shared table")
	}
	// The pin is gone, so a live writer now updates the table in place.
	if db.DetachPinnedTableForWrite("default", "big") {
		t.Fatal("live table is still pinned after the shadow took its copy")
	}
	big.Rows[0] = []any{0, "changed by another connection"}
	big.Version++

	recorded, err := base.Get("default", "big")
	if err != nil {
		t.Fatal(err)
	}
	if recorded == big {
		t.Fatal("base still aliases the live table after the pin was released")
	}
	if recorded.Version != 3 {
		t.Fatalf("base Version = %d, want the BEGIN-time 3", recorded.Version)
	}
	// A table the shadow never wrote stays pinned and is its own record.
	untouched, err := base.Get("default", "small")
	if err != nil {
		t.Fatal(err)
	}
	if untouched != small || !db.IsTablePinnedForStream(small) {
		t.Fatal("an unwritten table must stay pinned and shared with the base")
	}
	changes := CollectWALChanges(base, shadow)
	if len(changes) != 0 {
		t.Fatalf("a private copy that was not written produced %d changes", len(changes))
	}
}

// BEGIN reuses the catalog image while the catalog is at rest, rebuilds it after
// any mutation, and no transaction can reach the shared image through its
// private catalog.
func TestBeginCatalogImageIsReusedUntilCatalogChanges(t *testing.T) {
	db, _, _ := newSharedTestDB(t)
	if err := db.Catalog().RegisterView("main", "v1", "SELECT 1"); err != nil {
		t.Fatal(err)
	}
	begin := func() (*DB, func()) {
		db.LockContentForRead()
		defer db.UnlockContentForRead()
		_, shadow, release := db.SnapshotForTxShared()
		return shadow, release
	}
	first, release1 := begin()
	second, release2 := begin()
	image := db.Catalog().beginImage.Load()
	if image == nil || first.beginCatalog.Load() != image.snapshot || second.beginCatalog.Load() != image.snapshot {
		t.Fatal("two BEGINs over an unchanged catalog did not share one image")
	}
	// A transaction's catalog work stays on its private copy.
	if err := first.Catalog().RegisterView("main", "v_private", "SELECT 2"); err != nil {
		t.Fatal(err)
	}
	if !second.BeginCatalogSnapshot().Equal(db.SnapshotCatalog()) {
		t.Fatal("the shared image changed when a transaction changed its private catalog")
	}
	if first.SnapshotCatalog().Equal(first.BeginCatalogSnapshot()) {
		t.Fatal("the private catalog change is not visible to its own transaction")
	}
	release1()
	release2()

	if err := db.Catalog().RegisterView("main", "v2", "SELECT 3"); err != nil {
		t.Fatal(err)
	}
	third, release3 := begin()
	defer release3()
	if third.beginCatalog.Load() == image.snapshot {
		t.Fatal("BEGIN reused a catalog image from before a catalog mutation")
	}
	if !third.BeginCatalogSnapshot().Equal(db.SnapshotCatalog()) {
		t.Fatal("BEGIN after a catalog mutation does not see it")
	}
}

func TestBeginCatalogImageInvalidatesAfterRBACWrite(t *testing.T) {
	catalog := NewCatalogManager()
	before := catalog.beginSnapshot()
	if err := catalog.CreateRole("reader"); err != nil {
		t.Fatal(err)
	}
	after := catalog.beginSnapshot()
	if after == before {
		t.Fatal("RBAC mutation reused the previous catalog image")
	}
	if roles := diskToCatalog(before.dc).ListRoles(); len(roles) != 0 {
		t.Fatalf("previous image changed: %v", roles)
	}
	roles := diskToCatalog(after.dc).ListRoles()
	if len(roles) != 1 || roles[0].Name != "reader" {
		t.Fatalf("new image lost the role: %v", roles)
	}
	if catalog.beginSnapshot() != after {
		t.Fatal("unchanged RBAC catalog did not reuse its image")
	}
}
