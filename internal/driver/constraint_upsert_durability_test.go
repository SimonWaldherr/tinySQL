// Durability of ON CONFLICT ... DO UPDATE and CHECK constraints across a
// restart. An upsert that only updates existing rows changes no row count, so
// it must still mark those rows for the WAL and the checkpoint; CHECK metadata
// has to survive the reopen for later writes to be validated the same way.
package driver

import (
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"testing"
)

func inventoryRows(t *testing.T, q savepointQuerier) []string {
	t.Helper()
	rows, err := q.Query(`SELECT id, qty, note FROM inv`)
	if err != nil {
		t.Fatalf("select: %v", err)
	}
	defer rows.Close()
	out := []string{}
	for rows.Next() {
		var id, qty int
		var note string
		if err := rows.Scan(&id, &qty, &note); err != nil {
			t.Fatalf("scan: %v", err)
		}
		out = append(out, fmt.Sprintf("%d:%d:%s", id, qty, note))
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows: %v", err)
	}
	return out
}

func TestUpsertAndCheckConstraintsSurviveRestart(t *testing.T) {
	for _, mode := range savepointWALModes {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			dsn := "file:" + dir + "/inv?tenant=default&mode=" + mode

			db, err := sql.Open("tinysql", dsn)
			if err != nil {
				t.Fatalf("open: %v", err)
			}
			mustExec := func(q string) {
				t.Helper()
				if _, err := db.Exec(q); err != nil {
					t.Fatalf("exec %q: %v", q, err)
				}
			}
			mustExec(`CREATE TABLE inv (
				id INT PRIMARY KEY,
				sku TEXT UNIQUE,
				qty INT NOT NULL CHECK (qty >= 0),
				note TEXT
			)`)
			mustExec(`INSERT INTO inv VALUES (1, 'A1', 10, 'init'), (2, 'B2', 20, 'second')`)

			// Updates only: no row is inserted, so only the dirty-row bookkeeping
			// tells the WAL that anything changed.
			mustExec(`INSERT INTO inv VALUES (1, 'A1', 5, 'restock')
				ON CONFLICT (id) DO UPDATE SET qty = inv.qty + excluded.qty, note = excluded.note`)
			// Mixed insert and update in one statement.
			mustExec(`INSERT INTO inv VALUES (3, 'C3', 7, 'new'), (2, 'B2', 1, 'bump')
				ON CONFLICT (id) DO UPDATE SET qty = inv.qty + excluded.qty, note = excluded.note`)
			// Targeted DO NOTHING leaves the existing row alone.
			mustExec(`INSERT INTO inv VALUES (3, 'C3', 999, 'ignored') ON CONFLICT (id) DO NOTHING`)

			want := []string{"1:15:restock", "2:21:bump", "3:7:new"}
			if live := inventoryRows(t, db); !reflect.DeepEqual(live, want) {
				t.Fatalf("live rows = %v, want %v", live, want)
			}

			// Crash copy first: opening it replays the WAL that carries the
			// upserts. The clean restart below reads the checkpoint instead.
			crashDir := t.TempDir()
			copyDBFilesForCrash(t, dir, crashDir)
			if err := db.Close(); err != nil {
				t.Fatalf("close: %v", err)
			}

			crashed, err := sql.Open("tinysql", "file:"+crashDir+"/inv?tenant=default&mode="+mode)
			if err != nil {
				t.Fatalf("open crash copy: %v", err)
			}
			defer crashed.Close()
			if got := inventoryRows(t, crashed); !reflect.DeepEqual(got, want) {
				t.Fatalf("rows replayed from the WAL = %v, want %v", got, want)
			}
			assertInventoryChecksEnforced(t, crashed, want)

			reopened, err := sql.Open("tinysql", dsn)
			if err != nil {
				t.Fatalf("reopen: %v", err)
			}
			defer reopened.Close()
			if got := inventoryRows(t, reopened); !reflect.DeepEqual(got, want) {
				t.Fatalf("rows after clean restart = %v, want %v", got, want)
			}
			assertInventoryChecksEnforced(t, reopened, want)
		})
	}
}

// assertInventoryChecksEnforced confirms the CHECK clause still rejects plain
// and upsert writes, and that rejected writes leave the rows untouched.
func assertInventoryChecksEnforced(t *testing.T, db *sql.DB, want []string) {
	t.Helper()
	if _, err := db.Exec(`INSERT INTO inv VALUES (4, 'D4', -1, 'x')`); err == nil || !strings.Contains(err.Error(), "CHECK constraint failed") {
		t.Fatalf("INSERT violating CHECK: got %v", err)
	}
	if _, err := db.Exec(`INSERT INTO inv VALUES (1, 'A1', -100, 'x')
		ON CONFLICT (id) DO UPDATE SET qty = excluded.qty`); err == nil || !strings.Contains(err.Error(), "CHECK constraint failed") {
		t.Fatalf("upsert violating CHECK: got %v", err)
	}
	if got := inventoryRows(t, db); !reflect.DeepEqual(got, want) {
		t.Fatalf("rejected writes changed rows: %v", got)
	}
}
