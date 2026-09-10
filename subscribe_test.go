package tinysql_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	sql "github.com/SimonWaldherr/tinySQL"
	driver "github.com/SimonWaldherr/tinySQL/driver"
)

func nextChange(t *testing.T, s *sql.QuerySubscription) sql.QueryChange {
	t.Helper()
	select {
	case c, ok := <-s.Changes:
		if !ok {
			t.Fatalf("subscription stopped: %v", s.Err())
		}
		return c
	case <-time.After(5 * time.Second):
		t.Fatal("subscription timeout")
		return sql.QueryChange{}
	}
}
func execSubscriptionSQL(t *testing.T, db *sql.DB, query string) {
	t.Helper()
	if _, err := sql.ExecSQL(t.Context(), db, "default", query); err != nil {
		t.Fatal(err)
	}
}
func TestSubscriptionIncrementalAndOwnership(t *testing.T) {
	db := sql.NewDB()
	defer db.Close()
	execSubscriptionSQL(t, db, `CREATE TABLE items (id INT PRIMARY KEY, payload BLOB)`)
	execSubscriptionSQL(t, db, `INSERT INTO items VALUES (1,X'01'), (2,X'02')`)
	sub, err := sql.SubscribeSQL(t.Context(), db, "default", `SELECT id, payload FROM items WHERE id > 0`)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	first := nextChange(t, sub)
	if !first.Initial || len(first.Added) != 2 || first.ScannedRows != 2 {
		t.Fatal(first)
	}
	first.Added[0]["payload"].([]byte)[0] = 255
	execSubscriptionSQL(t, db, `INSERT INTO items VALUES (3,X'03'), (-1,X'00')`)
	add := nextChange(t, sub)
	if add.Initial || len(add.Added) != 1 || add.ScannedRows != 2 {
		t.Fatal(add)
	}
	execSubscriptionSQL(t, db, `UPDATE items SET payload = X'04' WHERE id = 1`)
	update := nextChange(t, sub)
	if update.ScannedRows != 1 || len(update.Added) != 1 || len(update.Removed) != 1 || update.Removed[0]["payload"].([]byte)[0] != 1 {
		t.Fatal(update)
	}
	execSubscriptionSQL(t, db, `DELETE FROM items WHERE id = 3`)
	deleted := nextChange(t, sub)
	if len(deleted.Removed) != 1 || deleted.Removed[0]["id"] != 3 {
		t.Fatal(deleted)
	}
	if sub.Err() != nil {
		t.Fatal(sub.Err())
	}
}
func TestSubscriptionTransactionsAndRollback(t *testing.T) {
	db := sql.NewDB()
	defer db.Close()
	execSubscriptionSQL(t, db, `CREATE TABLE events (id INT PRIMARY KEY)`)
	sub, err := sql.SubscribeSQL(t.Context(), db, "default", `SELECT id FROM events`)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	nextChange(t, sub)
	pool, err := driver.OpenWithDB(db)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	tx, err := pool.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`INSERT INTO events VALUES (1)`); err != nil {
		t.Fatal(err)
	}
	select {
	case c := <-sub.Changes:
		t.Fatalf("uncommitted event: %v", c)
	default:
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	tx, err = pool.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`INSERT INTO events VALUES (2), (3)`); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	change := nextChange(t, sub)
	if len(change.Added) != 2 || len(change.Removed) != 0 {
		t.Fatal(change)
	}
	for _, row := range change.Added {
		if row["id"] == 1 {
			t.Fatal("rolled back row leaked")
		}
	}
}
func TestSubscriptionSlowReaderAndShutdown(t *testing.T) {
	db := sql.NewDB()
	execSubscriptionSQL(t, db, `CREATE TABLE events (id INT)`)
	sub, err := sql.SubscribeSQL(t.Context(), db, "default", `SELECT id FROM events`)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	// Leave the initial result queued while many statements commit.
	for i := 0; i < 100; i++ {
		execSubscriptionSQL(t, db, fmt.Sprintf("INSERT INTO events VALUES (%d)", i))
	}
	total := 0
	for total < 100 {
		c := nextChange(t, sub)
		total += len(c.Added) - len(c.Removed)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case _, ok := <-sub.Changes:
		if ok {
			t.Fatal("unexpected event")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("close did not stop subscription")
	}
	if _, err := sql.SubscribeSQL(context.Background(), db, "default", `SELECT id FROM events`); err == nil {
		t.Fatal("closed DB accepted")
	}
}
func TestSubscriptionRejectsUnsupportedQueries(t *testing.T) {
	db := sql.NewDB()
	defer db.Close()
	execSubscriptionSQL(t, db, `CREATE TABLE items (id INT)`)
	for _, query := range []string{`SELECT COUNT(*) FROM items`, `SELECT id FROM items LIMIT 1`, `SELECT id FROM items ORDER BY id`, `SELECT NOW() FROM items`, `DELETE FROM items`} {
		if s, err := sql.SubscribeSQL(t.Context(), db, "default", query); err == nil {
			s.Close()
			t.Fatalf("accepted %s", query)
		}
	}
}

func TestSubscriptionDropStopsWithError(t *testing.T) {
	db := sql.NewDB()
	defer db.Close()
	execSubscriptionSQL(t, db, `CREATE TABLE items (id INT)`)
	sub, err := sql.SubscribeSQL(t.Context(), db, "default", `SELECT id FROM items`)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	nextChange(t, sub)
	execSubscriptionSQL(t, db, `DROP TABLE items`)
	select {
	case _, ok := <-sub.Changes:
		if ok || sub.Err() == nil {
			t.Fatal("missing terminal error")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("drop did not stop subscription")
	}
}
func TestSubscriptionCloseWithUnreadResults(t *testing.T) {
	db := sql.NewDB()
	execSubscriptionSQL(t, db, `CREATE TABLE items (id INT)`)
	sub, err := sql.SubscribeSQL(t.Context(), db, "default", `SELECT id FROM items`)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 20; i++ {
		execSubscriptionSQL(t, db, fmt.Sprintf("INSERT INTO items VALUES (%d)", i))
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	// Drain at most the bounded buffered result; DB closure must terminate
	// even if the worker was blocked delivering an additional change.
	timeout := time.After(5 * time.Second)
	for {
		select {
		case _, ok := <-sub.Changes:
			if !ok {
				sub.Close()
				return
			}
		case <-timeout:
			t.Fatal("close blocked by unread result")
		}
	}
}
