package storage

import (
	"sync"
	"testing"
)

func beginCatalogFixture(t *testing.T) *DB {
	t.Helper()
	db := NewDB()
	t.Cleanup(func() { _ = db.Close() })
	catalog := db.Catalog()
	for _, err := range []error{
		catalog.RegisterTable("main", "items", []Column{{Name: "id", Type: IntType}, {Name: "value", Type: TextType}}),
		catalog.RegisterView("main", "initial_view", "SELECT id FROM items"),
		catalog.RegisterIndexForTenant("default", &CatalogIndex{Schema: "main", Name: "items_key", Table: "items", Columns: []string{"id"}}),
		catalog.CreateRole("reader"),
		catalog.CreateRole("writer"),
		catalog.GrantPermission("reader", PermSelect, "main", "items"),
		catalog.GrantPermission("reader", PermUpdate, "main", "items"),
		catalog.CreateUser("alice", "catalog-test-password", []string{"reader"}),
	} {
		if err != nil {
			t.Fatal(err)
		}
	}
	return db
}

func TestBeginCatalogSnapshotTransactionIsolation(t *testing.T) {
	for _, constructor := range []struct {
		name string
		make func(*DB) (*DB, func())
	}{
		{"read", func(db *DB) (*DB, func()) { return db.SnapshotForReadTx(), func() {} }},
		{"write", func(db *DB) (*DB, func()) { return db.SnapshotForWriteTx(), func() {} }},
		{"pair", func(db *DB) (*DB, func()) { _, shadow := db.SnapshotForTx(); return shadow, func() {} }},
		{"shared-read", func(db *DB) (*DB, func()) { return db.SnapshotForReadTxShared() }},
		{"shared-pair", func(db *DB) (*DB, func()) { _, shadow, release := db.SnapshotForTxShared(); return shadow, release }},
	} {
		t.Run(constructor.name, func(t *testing.T) {
			db := beginCatalogFixture(t)
			before := db.SnapshotCatalog()
			db.LockContentForRead()
			shadow, release := constructor.make(db)
			db.UnlockContentForRead()
			t.Cleanup(release)
			if !shadow.IsShadow() || shadow.beginCatalog.Load() == nil || !shadow.BeginCatalogSnapshot().Equal(before) {
				t.Fatal("transaction snapshot did not retain its BEGIN catalog")
			}
			catalog := shadow.Catalog()
			for _, err := range []error{
				catalog.RegisterView("main", "private_view", "SELECT value FROM items"),
				catalog.RegisterIndexForTenant("default", &CatalogIndex{Schema: "main", Name: "items_key", Table: "items", Columns: []string{"value"}}),
				catalog.RevokePermission("reader", PermSelect, "main", "items"),
				catalog.GrantPermission("reader", PermDelete, "main", "items"),
				catalog.GrantRoleToUser("alice", "writer"),
			} {
				if err != nil {
					t.Fatal(err)
				}
			}
			if !db.SnapshotCatalog().Equal(before) || !shadow.BeginCatalogSnapshot().Equal(before) {
				t.Fatal("shadow DDL/index/RBAC changes mutated the live or BEGIN catalog")
			}
			afterShadow := shadow.SnapshotCatalog()
			if afterShadow.Equal(before) {
				t.Fatal("shadow catalog mutations were not visible in its current snapshot")
			}
			if err := db.Catalog().RegisterView("main", "live_view", "SELECT 2"); err != nil {
				t.Fatal(err)
			}
			if !shadow.SnapshotCatalog().Equal(afterShadow) || !shadow.BeginCatalogSnapshot().Equal(before) {
				t.Fatal("live DDL changed the transaction snapshot")
			}
		})
	}
}

func TestBeginCatalogSnapshotCurrentOutsideTransaction(t *testing.T) {
	for _, clone := range []bool{false, true} {
		db := beginCatalogFixture(t)
		if clone {
			db = db.DeepClone()
		}
		if db.beginCatalog.Load() != nil {
			t.Fatal("ordinary database/deep clone retained a BEGIN image")
		}
		before := db.BeginCatalogSnapshot()
		if err := db.Catalog().RegisterView("main", "later_view", "SELECT 2"); err != nil {
			t.Fatal(err)
		}
		after := db.BeginCatalogSnapshot()
		if after.Equal(before) || !after.Equal(db.SnapshotCatalog()) {
			t.Fatal("ordinary database/deep clone returned an obsolete catalog")
		}
	}
	var nilDB *DB
	if !nilDB.BeginCatalogSnapshot().Equal(nilDB.SnapshotCatalog()) {
		t.Fatal("nil database snapshot changed")
	}
}

func TestBeginCatalogSnapshotPromotionReleasesImage(t *testing.T) {
	db := beginCatalogFixture(t)
	shadow := db.SnapshotForWriteTx()
	before := shadow.BeginCatalogSnapshot()
	if err := shadow.Catalog().RegisterView("main", "committed_view", "SELECT 3"); err != nil {
		t.Fatal(err)
	}
	current := shadow.SnapshotCatalog()
	start := make(chan struct{})
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			<-start
			for range 20 {
				got := shadow.BeginCatalogSnapshot()
				if !got.Equal(before) && !got.Equal(current) {
					t.Error("promotion exposed an unrelated catalog snapshot")
				}
			}
		})
	}
	close(start)
	shadow.PromoteShadow()
	wg.Wait()
	if shadow.IsShadow() || shadow.beginCatalog.Load() != nil || !shadow.BeginCatalogSnapshot().Equal(current) {
		t.Fatal("promotion retained the BEGIN image or returned its old contents")
	}
	if err := shadow.Catalog().RegisterView("main", "post_commit_view", "SELECT 4"); err != nil {
		t.Fatal(err)
	}
	if !shadow.BeginCatalogSnapshot().Equal(shadow.SnapshotCatalog()) || shadow.BeginCatalogSnapshot().Equal(current) {
		t.Fatal("promoted database did not snapshot its latest catalog")
	}
}
