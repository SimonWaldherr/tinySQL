// Durability of SAVEPOINT / ROLLBACK TO / RELEASE across a restart.
//
// Savepoints restore the transaction's in-memory shadow, but the WAL is a
// separate record of the same work. ModeAdvancedWAL logs row operations as they
// run and replays them by appending inserts, so a discarded row must be undone
// in the log, and the restored shadow must keep logging into the transaction
// that COMMIT will close. These tests reopen the database after each
// transaction and compare what a restart sees with the live state.
package driver

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

type savepointQuerier interface {
	Query(query string, args ...any) (*sql.Rows, error)
}

// savepointRows returns "id:val" for every row of t in storage order, which is
// the order a restart has to reproduce.
func savepointRows(t *testing.T, q savepointQuerier) []string {
	t.Helper()
	rows, err := q.Query(`SELECT id, val FROM t`)
	if err != nil {
		t.Fatalf("select: %v", err)
	}
	defer rows.Close()
	out := []string{}
	for rows.Next() {
		var id int
		var val string
		if err := rows.Scan(&id, &val); err != nil {
			t.Fatalf("scan: %v", err)
		}
		out = append(out, fmt.Sprintf("%d:%s", id, val))
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows: %v", err)
	}
	return out
}

type savepointStep struct {
	sql     string
	wantErr string // substring the statement must fail with; empty means success
}

// copyDBFilesForCrash copies the files of a database directory while the
// database is still open. The copy is what a process that died at that instant
// would leave behind: the last checkpoint plus whatever the WAL holds, so
// opening it replays the log. A clean Close checkpoints and truncates the WAL,
// which is why the restart checks below need this copy as well.
func copyDBFilesForCrash(t *testing.T, srcDir, dstDir string) {
	t.Helper()
	entries, err := os.ReadDir(srcDir)
	if err != nil {
		t.Fatalf("read %s: %v", srcDir, err)
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		data, err := os.ReadFile(filepath.Join(srcDir, e.Name()))
		if err != nil {
			t.Fatalf("read %s: %v", e.Name(), err)
		}
		if err := os.WriteFile(filepath.Join(dstDir, e.Name()), data, 0o600); err != nil {
			t.Fatalf("write %s: %v", e.Name(), err)
		}
	}
}

func runSavepointDurabilityScenario(t *testing.T, mode string, setup []string, tx []savepointStep, want []string) {
	t.Helper()
	dir := t.TempDir()
	dsn := "file:" + dir + "/sp?tenant=default&mode=" + mode

	db, err := sql.Open("tinysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	for _, q := range setup {
		if _, err := db.Exec(q); err != nil {
			t.Fatalf("setup %q: %v", q, err)
		}
	}

	txn, err := db.Begin()
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	for _, step := range tx {
		_, err := txn.Exec(step.sql)
		switch {
		case step.wantErr == "" && err != nil:
			t.Fatalf("%s: %q: %v", mode, step.sql, err)
		case step.wantErr != "" && (err == nil || !strings.Contains(err.Error(), step.wantErr)):
			t.Fatalf("%s: %q: want error containing %q, got %v", mode, step.sql, step.wantErr, err)
		}
	}
	if err := txn.Commit(); err != nil {
		t.Fatalf("%s: commit: %v", mode, err)
	}
	if live := savepointRows(t, db); !reflect.DeepEqual(live, want) {
		t.Fatalf("%s: live rows = %v, want %v", mode, live, want)
	}

	crashDir := t.TempDir()
	copyDBFilesForCrash(t, dir, crashDir)
	if err := db.Close(); err != nil {
		t.Fatalf("%s: close: %v", mode, err)
	}

	crashed, err := sql.Open("tinysql", "file:"+crashDir+"/sp?tenant=default&mode="+mode)
	if err != nil {
		t.Fatalf("%s: open crash copy: %v", mode, err)
	}
	if got := savepointRows(t, crashed); !reflect.DeepEqual(got, want) {
		t.Fatalf("%s: rows replayed from the WAL = %v, want %v", mode, got, want)
	}
	if err := crashed.Close(); err != nil {
		t.Fatalf("%s: close crash copy: %v", mode, err)
	}

	reopened, err := sql.Open("tinysql", dsn)
	if err != nil {
		t.Fatalf("%s: reopen: %v", mode, err)
	}
	defer reopened.Close()
	if got := savepointRows(t, reopened); !reflect.DeepEqual(got, want) {
		t.Fatalf("%s: rows after clean restart = %v, want %v", mode, got, want)
	}
}

// savepointWALModes are the file-backed WAL modes whose durability the
// savepoint restore must preserve.
var savepointWALModes = []string{"wal", "advancedwal"}

func TestSavepointRollbackIsDurableAcrossRestart(t *testing.T) {
	createT := `CREATE TABLE t (id INT, val TEXT)`
	cases := []struct {
		name  string
		setup []string
		tx    []savepointStep
		want  []string
	}{
		{
			name:  "updates and deletes after the savepoint are undone",
			setup: []string{createT, `INSERT INTO t VALUES (1, 'a')`, `INSERT INTO t VALUES (2, 'b')`, `INSERT INTO t VALUES (3, 'c')`},
			tx: []savepointStep{
				{sql: `SAVEPOINT s`},
				{sql: `UPDATE t SET val = 'x' WHERE id = 1`},
				{sql: `DELETE FROM t WHERE id = 2`},
				{sql: `INSERT INTO t VALUES (4, 'd')`},
				{sql: `ROLLBACK TO SAVEPOINT s`},
				{sql: `INSERT INTO t VALUES (5, 'e')`},
			},
			want: []string{"1:a", "2:b", "3:c", "5:e"},
		},
		{
			name:  "savepoint taken before the transaction's first write",
			setup: []string{createT, `INSERT INTO t VALUES (1, 'a')`},
			tx: []savepointStep{
				{sql: `SAVEPOINT s`},
				{sql: `INSERT INTO t VALUES (2, 'b')`},
				{sql: `ROLLBACK TO s`},
				{sql: `INSERT INTO t VALUES (3, 'c')`},
			},
			want: []string{"1:a", "3:c"},
		},
		{
			name:  "nested savepoints discard their inner work",
			setup: []string{createT, `INSERT INTO t VALUES (1, 'a')`},
			tx: []savepointStep{
				{sql: `SAVEPOINT outer_sp`},
				{sql: `INSERT INTO t VALUES (2, 'b')`},
				{sql: `SAVEPOINT inner_sp`},
				{sql: `DELETE FROM t WHERE id = 1`},
				{sql: `UPDATE t SET val = 'B' WHERE id = 2`},
				{sql: `ROLLBACK TO inner_sp`},
				{sql: `INSERT INTO t VALUES (3, 'c')`},
				{sql: `ROLLBACK TO outer_sp`},
				{sql: `INSERT INTO t VALUES (4, 'd')`},
			},
			want: []string{"1:a", "4:d"},
		},
		{
			name:  "the same savepoint can be rolled back to repeatedly",
			setup: []string{createT},
			tx: []savepointStep{
				{sql: `INSERT INTO t VALUES (1, 'a')`},
				{sql: `SAVEPOINT s`},
				{sql: `INSERT INTO t VALUES (2, 'b')`},
				{sql: `ROLLBACK TO s`},
				{sql: `INSERT INTO t VALUES (3, 'c')`},
				{sql: `ROLLBACK TO s`},
				{sql: `INSERT INTO t VALUES (4, 'd')`},
				{sql: `RELEASE s`},
			},
			want: []string{"1:a", "4:d"},
		},
		{
			name:  "rows written before the savepoint survive a rollback to it",
			setup: []string{createT},
			tx: []savepointStep{
				{sql: `INSERT INTO t VALUES (1, 'a')`},
				{sql: `SAVEPOINT s`},
				{sql: `INSERT INTO t VALUES (2, 'b')`},
				{sql: `ROLLBACK TO s`},
			},
			want: []string{"1:a"},
		},
		{
			name:  "rolling back a single update leaves the committed row",
			setup: []string{createT, `INSERT INTO t VALUES (1, 'a')`},
			tx: []savepointStep{
				{sql: `SAVEPOINT s`},
				{sql: `UPDATE t SET val = 'z' WHERE id = 1`},
				{sql: `ROLLBACK TO s`},
				{sql: `RELEASE s`},
			},
			want: []string{"1:a"},
		},
	}
	for _, tc := range cases {
		for _, mode := range savepointWALModes {
			t.Run(tc.name+"/"+mode, func(t *testing.T) {
				runSavepointDurabilityScenario(t, mode, tc.setup, tc.tx, tc.want)
			})
		}
	}
}

// A schema change cannot be undone in an AdvancedWAL record. The rollback is
// refused before anything is logged, so the transaction keeps all its work and
// commits it. Basic WAL logs the final state of the transaction and accepts the
// same rollback.
func TestSavepointRollbackRefusesSchemaChangeOnAdvancedWAL(t *testing.T) {
	runSavepointDurabilityScenario(t, "advancedwal",
		[]string{`CREATE TABLE t (id INT, val TEXT)`, `INSERT INTO t VALUES (1, 'a')`},
		[]savepointStep{
			{sql: `SAVEPOINT s`},
			{sql: `INSERT INTO t VALUES (2, 'b')`},
			{sql: `CREATE TABLE u (x INT)`},
			{sql: `ROLLBACK TO s`, wantErr: "CREATE, DROP or ALTER TABLE"},
			{sql: `INSERT INTO t VALUES (3, 'c')`},
		},
		[]string{"1:a", "2:b", "3:c"},
	)
}

func TestSavepointRollbackAcceptsSchemaChangeOnBasicWAL(t *testing.T) {
	runSavepointDurabilityScenario(t, "wal",
		[]string{`CREATE TABLE t (id INT, val TEXT)`, `INSERT INTO t VALUES (1, 'a')`},
		[]savepointStep{
			{sql: `SAVEPOINT s`},
			{sql: `INSERT INTO t VALUES (2, 'b')`},
			{sql: `CREATE TABLE u (x INT)`},
			{sql: `ROLLBACK TO s`},
			{sql: `INSERT INTO t VALUES (3, 'c')`},
		},
		[]string{"1:a", "3:c"},
	)
}
