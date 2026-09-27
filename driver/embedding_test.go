package driver_test

import (
	"context"
	"testing"

	tinysql "github.com/SimonWaldherr/tinySQL"
	"github.com/SimonWaldherr/tinySQL/driver"
)

func TestEmbeddedPoolsStayBoundToTheirDatabase(t *testing.T) {
	ctx := context.Background()
	originalDefault := driver.CurrentDefaultDB()
	first, second := tinysql.NewDB(), tinysql.NewDB()
	defer first.Close()
	defer second.Close()
	a, err := driver.OpenWithDB(first)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	b, err := driver.OpenWithDB(second)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	if driver.CurrentDefaultDB() != originalDefault {
		t.Fatal("OpenWithDB changed the global default")
	}
	// Both handles are still lazy: opening b must not redirect a's first connection.
	for i, db := range []*tinysql.DB{first, second} {
		if _, err := tinysql.ExecSQL(ctx, db, "default", "CREATE TABLE identity (id INT)"); err != nil {
			t.Fatal(err)
		}
		value := "11"
		if i == 1 {
			value = "22"
		}
		if _, err := tinysql.ExecSQL(ctx, db, "default", "INSERT INTO identity VALUES ("+value+")"); err != nil {
			t.Fatal(err)
		}
	}
	var got int
	if err := a.QueryRowContext(ctx, "SELECT id FROM identity").Scan(&got); err != nil || got != 11 {
		t.Fatalf("first pool: id=%d, err=%v", got, err)
	}
	if err := b.QueryRowContext(ctx, "SELECT id FROM identity").Scan(&got); err != nil || got != 22 {
		t.Fatalf("second pool: id=%d, err=%v", got, err)
	}
	held, err := a.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer held.Close()
	// Force a second physical connection while the first is checked out.
	if err := a.QueryRowContext(ctx, "SELECT id FROM identity").Scan(&got); err != nil || got != 11 {
		t.Fatalf("new pooled connection: id=%d, err=%v", got, err)
	}
	if err := held.Close(); err != nil {
		t.Fatal(err)
	}
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	if first.IsClosed() {
		t.Fatal("closing SQL pool closed caller-owned database")
	}
	if _, err := tinysql.ExecSQL(ctx, first, "default", "INSERT INTO identity VALUES (33)"); err != nil {
		t.Fatalf("native DB unusable after pool close: %v", err)
	}
}

func TestOpenWithDBRejectsInvalidDatabase(t *testing.T) {
	if db, err := driver.OpenWithDB(nil); err == nil || db != nil {
		t.Fatal("nil database accepted")
	}
	closed := tinysql.NewDB()
	if err := closed.Close(); err != nil {
		t.Fatal(err)
	}
	if db, err := driver.OpenWithDB(closed); err == nil || db != nil {
		t.Fatal("closed database accepted")
	}
}

// A caller-supplied database with a durable backend must persist each
// acknowledged write, like the equivalent mode= DSN, not only on Close.
func TestOpenWithDBPersistsDurableModesPerWrite(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	native, err := tinysql.OpenDB(tinysql.StorageConfig{Mode: tinysql.ModeJSON, Path: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer native.Close()
	pool, err := driver.OpenWithDB(native)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	for _, statement := range []string{"CREATE TABLE notes (id INT, body TEXT)", "INSERT INTO notes VALUES (1, 'kept')"} {
		if _, err := pool.ExecContext(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}
	// Read the files through an independent read-only database while the
	// writer is still open.
	reader, err := tinysql.OpenDB(tinysql.StorageConfig{Mode: tinysql.ModeJSON, Path: dir, ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	rs, err := tinysql.ExecSQL(ctx, reader, "default", "SELECT body FROM notes WHERE id = 1")
	if err != nil {
		t.Fatal(err)
	}
	if len(rs.Rows) != 1 || rs.Rows[0]["body"] != "kept" {
		t.Fatalf("write not persisted before close: %#v", rs.Rows)
	}
}
