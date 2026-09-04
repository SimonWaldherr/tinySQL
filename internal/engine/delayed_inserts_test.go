package engine

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestDelayedInsertsLifecycle(t *testing.T) {
	db := storage.NewDB()
	execSQL(t, db, `CREATE TABLE docs (id INT PRIMARY KEY, body TEXT)`)
	rs := execSQL(t, db, `CALL DELAY_INSERT('24h', 'INSERT INTO docs VALUES (1, ''hello'')')`)
	id := rs.Rows[0]["id"].(string)
	if len(execSQL(t, db, `SELECT * FROM docs`).Rows) != 0 {
		t.Fatal("insert was not delayed")
	}
	if got := execSQL(t, db, `CALL FLUSH_DELAYED_INSERTS(10)`).Rows[0]["processed"]; got != 0 {
		t.Fatal(got)
	}
	if len(execSQL(t, db, `CALL DELAYED_INSERTS()`).Rows) != 1 {
		t.Fatal("missing queued insert")
	}
	if len(execSQL(t, db, `SELECT * FROM sys.tables WHERE name = '__tiny_delayed_inserts'`).Rows) != 0 {
		t.Fatal("queue is not hidden")
	}
	execSQL(t, db, fmt.Sprintf(`CALL RESCHEDULE_DELAYED_INSERT('%s', '0s')`, id))
	execSQL(t, db, `CALL FLUSH_DELAYED_INSERTS(10)`)
	if got := execSQL(t, db, `SELECT body FROM docs`).Rows[0]["body"]; got != "hello" {
		t.Fatal(got)
	}
	if len(execSQL(t, db, `CALL DELAYED_INSERTS()`).Rows) != 0 {
		t.Fatal("queue not acknowledged")
	}
	execSQL(t, db, `CALL FLUSH_DELAYED_INSERTS(10)`)
	if len(execSQL(t, db, `SELECT * FROM docs`).Rows) != 1 {
		t.Fatal("duplicate delivery")
	}
}

func TestDelayedInsertFrozenValuesAndPersistence(t *testing.T) {
	db := storage.NewDB()
	execSQL(t, db, `CREATE TABLE source (id INT, body TEXT)`)
	execSQL(t, db, `CREATE TABLE dest (id INT, body TEXT)`)
	execSQL(t, db, `INSERT INTO source VALUES (1, 'before'), (2, NULL)`)
	execSQL(t, db, `CALL DELAY_INSERT('0s', 'INSERT INTO dest SELECT * FROM source')`)
	execSQL(t, db, `UPDATE source SET body = 'after'`)
	// Save/load must carry the actual queued values, not just a SQL expression.
	path := t.TempDir() + "/queue.db"
	if err := storage.SaveToFile(db, path); err != nil {
		t.Fatal(err)
	}
	loaded, err := storage.LoadFromFile(path)
	if err != nil {
		t.Fatal(err)
	}
	execSQL(t, loaded, `CALL FLUSH_DELAYED_INSERTS(10)`)
	rows := execSQL(t, loaded, `SELECT body FROM dest ORDER BY id`).Rows
	if len(rows) != 2 || rows[0]["body"] != "before" || rows[1]["body"] != nil {
		t.Fatal(rows)
	}
}

func TestDelayedInsertBatchRollbackAndCancel(t *testing.T) {
	db := storage.NewDB()
	execSQL(t, db, `CREATE TABLE d (id INT PRIMARY KEY)`)
	execSQL(t, db, `INSERT INTO d VALUES (2)`)
	execSQL(t, db, `CALL DELAY_INSERT('0s', 'INSERT INTO d VALUES (1)')`)
	bad := execSQL(t, db, `CALL DELAY_INSERT('0s', 'INSERT INTO d VALUES (2)')`).Rows[0]["id"]
	if _, err := Execute(context.Background(), db, "default", mustParse(`CALL FLUSH_DELAYED_INSERTS(10)`)); err == nil {
		t.Fatal("expected duplicate error")
	}
	if len(execSQL(t, db, `SELECT * FROM d`).Rows) != 1 || len(execSQL(t, db, `CALL DELAYED_INSERTS()`).Rows) != 2 {
		t.Fatal("failed batch was not rolled back")
	}
	execSQL(t, db, fmt.Sprintf(`CALL CANCEL_DELAYED_INSERT('%s')`, bad))
	execSQL(t, db, `CALL FLUSH_DELAYED_INSERTS(1)`)
	if len(execSQL(t, db, `SELECT * FROM d`).Rows) != 2 {
		t.Fatal("retry lost data")
	}
}

func TestDelayedInsertConcurrentDrain(t *testing.T) {
	db := storage.NewDB()
	execSQL(t, db, `CREATE TABLE d (id INT)`)
	for i := 0; i < 10; i++ {
		execSQL(t, db, fmt.Sprintf(`CALL DELAY_INSERT('0s', 'INSERT INTO d VALUES (%d)')`, i))
	}
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := Execute(context.Background(), db, "default", mustParse(`CALL FLUSH_DELAYED_INSERTS(3)`)); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if len(execSQL(t, db, `SELECT * FROM d`).Rows) != 10 {
		t.Fatal("lost or duplicated rows")
	}
}

func TestDelayedInsertValidation(t *testing.T) {
	db := storage.NewDB()
	execSQL(t, db, `CREATE TABLE d (id INT)`)
	for _, sql := range []string{
		`CALL DELAY_INSERT('-1s', 'INSERT INTO d VALUES (1)')`,
		`CALL DELAY_INSERT('tomorrow', 'INSERT INTO d VALUES (1)')`,
		`CALL DELAY_INSERT('0s', 'DELETE FROM d')`,
		`CALL DELAY_INSERT('0s', 'INSERT INTO d VALUES (1, 2)')`,
		`CALL DELAY_INSERT('0s', 'INSERT INTO d VALUES (1) RETURNING id')`,
		`CALL FLUSH_DELAYED_INSERTS(0)`,
		`CALL FLUSH_DELAYED_INSERTS(1.5)`,
	} {
		if _, err := Execute(context.Background(), db, "default", mustParse(sql)); err == nil {
			t.Errorf("accepted %s", sql)
		}
	}
	now := time.Now()
	at, err := delayedInsertTime("2099-01-01T00:00:00Z", now)
	if err != nil || at.Year() != 2099 {
		t.Fatal(at, err)
	}
}

func TestDelayedInsertStorageModes(t *testing.T) {
	for _, mode := range []storage.StorageMode{storage.ModeDisk, storage.ModeWAL, storage.ModeAdvancedWAL} {
		t.Run(fmt.Sprint(mode), func(t *testing.T) {
			cfg := storage.DefaultStorageConfig(mode)
			cfg.Path = t.TempDir() + "/db"
			db, err := storage.OpenDB(cfg)
			if err != nil {
				t.Fatal(err)
			}
			execSQL(t, db, `CREATE TABLE d (id INT PRIMARY KEY)`)
			execSQL(t, db, `CALL DELAY_INSERT('0s', 'INSERT INTO d VALUES (1)')`)
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			db, err = storage.OpenDB(cfg)
			if err != nil {
				t.Fatal(err)
			}
			execSQL(t, db, `CALL FLUSH_DELAYED_INSERTS(10)`)
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			db, err = storage.OpenDB(cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			if len(execSQL(t, db, `SELECT * FROM d`).Rows) != 1 || len(execSQL(t, db, `CALL DELAYED_INSERTS()`).Rows) != 0 {
				t.Fatalf("delivery and acknowledgment did not persist: dest=%#v queue=%#v", execSQL(t, db, `SELECT * FROM d`).Rows, execSQL(t, db, `CALL DELAYED_INSERTS()`).Rows)
			}
		})
	}
}

func TestDelayedInsertSchemaChangeAndTenantIsolation(t *testing.T) {
	db := storage.NewDB()
	execSQL(t, db, `CREATE TABLE d (id INT)`)
	execSQL(t, db, `CALL DELAY_INSERT('0s', 'INSERT INTO d VALUES (1)')`)
	rs, err := Execute(context.Background(), db, "other", mustParse(`CALL FLUSH_DELAYED_INSERTS(10)`))
	if err != nil || rs.Rows[0]["processed"] != 0 {
		t.Fatal(rs, err)
	}
	execSQL(t, db, `ALTER TABLE d ADD COLUMN more TEXT`)
	if _, err := Execute(context.Background(), db, "default", mustParse(`CALL FLUSH_DELAYED_INSERTS(10)`)); err == nil {
		t.Fatal("schema change must fail")
	}
	if len(execSQL(t, db, `CALL DELAYED_INSERTS()`).Rows) != 1 {
		t.Fatal("failed job lost")
	}
}

func TestDelayedInsertChecksSubmitterAfterRevoke(t *testing.T) {
	db := storage.NewDB()
	execSQL(t, db, `CREATE TABLE d (id INT)`)
	cat := db.Catalog()
	if err := cat.CreateRole("writer"); err != nil {
		t.Fatal(err)
	}
	if err := cat.GrantPermission("writer", storage.PermInsert, "*", "*"); err != nil {
		t.Fatal(err)
	}
	if err := cat.CreateUser("submitter", "pw", []string{"writer"}); err != nil {
		t.Fatal(err)
	}
	if err := cat.CreateUser("worker", "pw", []string{"writer"}); err != nil {
		t.Fatal(err)
	}
	submitter := WithUser(context.Background(), "submitter")
	worker := WithUser(context.Background(), "worker")
	if _, err := Execute(submitter, db, "default", mustParse(`CALL DELAY_INSERT('0s', 'INSERT INTO d VALUES (1)')`)); err != nil {
		t.Fatal(err)
	}
	if err := cat.SetUserDisabled("submitter", true); err != nil {
		t.Fatal(err)
	}
	if _, err := Execute(worker, db, "default", mustParse(`CALL FLUSH_DELAYED_INSERTS(10)`)); err == nil {
		t.Fatal("disabled submitter's work executed")
	}
	table, err := db.Get("default", "d")
	if err != nil || len(table.Rows) != 0 {
		t.Fatal("failed authorization modified target")
	}
}

func TestDelayedInsertRunsDefaultsTriggersAndIndexesAtDelivery(t *testing.T) {
	db := storage.NewDB()
	for _, sql := range []string{
		`CREATE TABLE d (id INT PRIMARY KEY, amount INT DEFAULT 7)`,
		`CREATE INDEX amount_idx ON d(amount)`,
		`CREATE TABLE audit (id INT, amount INT)`,
		`CREATE TRIGGER inserted AFTER INSERT ON d BEGIN INSERT INTO audit VALUES (NEW.id, NEW.amount); END`,
		`CALL DELAY_INSERT('2001-01-01T00:00:00Z', 'INSERT INTO d (id) VALUES (2)')`,
		`CALL DELAY_INSERT('2000-01-01T00:00:00Z', 'INSERT INTO d (id) VALUES (1)')`,
	} {
		execSQL(t, db, sql)
	}
	if len(execSQL(t, db, `SELECT * FROM audit`).Rows) != 0 {
		t.Fatal("trigger ran before delivery")
	}
	execSQL(t, db, `CALL FLUSH_DELAYED_INSERTS(1)`)
	rows := execSQL(t, db, `SELECT id FROM d WHERE amount = 7`).Rows
	if len(rows) != 1 {
		t.Fatal(rows)
	}
	expectInt(t, rows[0]["id"], 1, "earliest due job")
	audit := execSQL(t, db, `SELECT amount FROM audit`).Rows
	if len(audit) != 1 {
		t.Fatal(audit)
	}
	expectInt(t, audit[0]["amount"], 7, "default visible to trigger")
	if len(execSQL(t, db, `CALL DELAYED_INSERTS()`).Rows) != 1 {
		t.Fatal("batch limit ignored")
	}
}
