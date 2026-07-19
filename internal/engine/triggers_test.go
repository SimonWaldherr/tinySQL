package engine

import (
	"context"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestTriggerWhenCondition(t *testing.T) {
	db := storage.NewDB()
	ctx := context.Background()

	for _, sql := range []string{
		`CREATE TABLE orders (id INT, amount INT)`,
		`CREATE TABLE audit_log (id INT, note TEXT)`,
		`CREATE TRIGGER audit_large_order AFTER INSERT ON orders
			FOR EACH ROW WHEN (NEW.amount > 100)
			BEGIN
				INSERT INTO audit_log VALUES (NEW.id, 'large');
			END`,
		`INSERT INTO orders VALUES (1, 50)`,
		`INSERT INTO orders VALUES (2, 150)`,
	} {
		if _, err := Execute(ctx, db, "default", mustParse(sql)); err != nil {
			t.Fatalf("execute %q: %v", sql, err)
		}
	}

	rs, err := Execute(ctx, db, "default", mustParse(`SELECT id, note FROM audit_log`))
	if err != nil {
		t.Fatalf("select audit_log: %v", err)
	}
	if len(rs.Rows) != 1 {
		t.Fatalf("expected one trigger row, got %#v", rs.Rows)
	}
	if rs.Rows[0]["id"] != 2 || rs.Rows[0]["note"] != "large" {
		t.Fatalf("unexpected trigger row: %#v", rs.Rows[0])
	}
}

// TestDropTriggerPurgesCache guards against a leak: triggerBodyCache and
// triggerWhenCache are keyed by trigger name and, before this fix, were only
// ever populated and never cleaned up, so a long-running deployment that
// creates/drops triggers dynamically would grow both maps forever. DROP
// TRIGGER must purge the entries for the dropped name from both caches.
func TestDropTriggerPurgesCache(t *testing.T) {
	db := storage.NewDB()
	ctx := context.Background()

	const name = "trg_cache_purge_test"
	for _, sql := range []string{
		`CREATE TABLE orders_cache_test (id INT, amount INT)`,
		`CREATE TABLE audit_log_cache_test (id INT, note TEXT)`,
		`CREATE TRIGGER ` + name + ` AFTER INSERT ON orders_cache_test
			FOR EACH ROW WHEN (NEW.amount > 100)
			BEGIN
				INSERT INTO audit_log_cache_test VALUES (NEW.id, 'large');
			END`,
		// Fire the trigger so triggerBodyStatements/triggerWhenExpr populate
		// their caches (CREATE TRIGGER itself already caches both, but this
		// also exercises the read path).
		`INSERT INTO orders_cache_test VALUES (1, 150)`,
	} {
		if _, err := Execute(ctx, db, "default", mustParse(sql)); err != nil {
			t.Fatalf("execute %q: %v", sql, err)
		}
	}

	triggerCacheMu.RLock()
	_, bodyCached := triggerBodyCache[name]
	_, whenCached := triggerWhenCache[name]
	triggerCacheMu.RUnlock()
	if !bodyCached {
		t.Fatalf("expected triggerBodyCache to hold an entry for %q before DROP TRIGGER", name)
	}
	if !whenCached {
		t.Fatalf("expected triggerWhenCache to hold an entry for %q before DROP TRIGGER", name)
	}

	if _, err := Execute(ctx, db, "default", mustParse(`DROP TRIGGER `+name)); err != nil {
		t.Fatalf("drop trigger: %v", err)
	}

	triggerCacheMu.RLock()
	_, bodyCached = triggerBodyCache[name]
	_, whenCached = triggerWhenCache[name]
	triggerCacheMu.RUnlock()
	if bodyCached {
		t.Fatalf("triggerBodyCache still holds an entry for %q after DROP TRIGGER", name)
	}
	if whenCached {
		t.Fatalf("triggerWhenCache still holds an entry for %q after DROP TRIGGER", name)
	}
}
