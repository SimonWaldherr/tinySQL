// Golden WAL fixture generator — WALManager (ModeWAL) side.
//
// This is Stage 0 of the WAL-consolidation characterization effort (see
// internal/storage/wal_fixture_test.go for the replay-side verification and
// internal/storage/wal_crash_harness_test.go for crash simulation). It is a
// one-shot generator, not part of the normal test suite — run it explicitly
// to (re)produce internal/storage/testdata/wal_fixtures/walmanager_legacy.wal:
//
//	TINYSQL_REGEN_WAL_FIXTURES=1 go test ./internal/driver/ -run TestGenerateWALManagerFixture -v
//
// Left unguarded this would silently rewrite the checked-in golden file
// every time `go test ./...` runs: its bytes embed wall-clock timestamps
// and are therefore never byte-stable across runs, which is not what a
// "golden" characterization fixture should do.
//
// Every statement below runs through the real database/sql driver against
// a mode=wal DSN — sql.Open + Exec/Begin/Commit — exactly the path a real
// caller uses (see TestDriverModeJSONPersistsAndReopens for the same
// pattern against a different mode). No walRecord or WALChange value is
// ever hand-constructed.
//
// One deliberate wrinkle: the very first write against a brand-new,
// empty ModeWAL database is wrapped in its own explicit transaction
// instead of a plain autocommit Exec. This works around a real bug this
// characterization effort surfaced: storage.DB.ShallowCloneForTable
// special-cases an empty db.tenants map by returning a bare NewDB()
// *without copying the .wal field* (see db.go's ShallowCloneForTable,
// which conn.execStatement's autocommit path uses for every non-tx
// write). Since a freshly opened ModeWAL database always starts with
// zero tables, its very first autocommit write permanently detaches
// WALManager from db.wal for the rest of that process's life — silently,
// with no error — because commitTx/execStatement never restores it
// afterwards. Routing that first write through an explicit BEGIN/COMMIT
// avoids the bug (storage.DB.SnapshotForTx, used by BeginTx, copies .wal
// unconditionally, with no empty-tenants special case), matching what a
// caller who always uses explicit transactions would see, and lets every
// later statement in this generator use plain autocommit safely, since by
// then db.tenants is no longer empty and ShallowCloneForTable behaves
// correctly. See internal/storage/wal_fixture_test.go's package doc
// comment for the full writeup — this is a real, currently-shipping gap,
// not a fixture-generation artifact, and it is entirely in db.go, which
// this Stage 0 harness must not modify.
//
// A second, separate finding surfaced the same way: every driver-mediated
// write against ModeWAL — autocommit or explicit-transaction — logs its
// diff to the WAL file *twice* (once via engine.Execute's own automatic
// per-statement logging, once via the driver's own manual diff-and-log).
// See internal/storage/wal_fixture_test.go's doc comment for the full
// writeup and why it doesn't affect this fixture's final-state correctness.
package driver

import (
	"database/sql"
	"os"
	"path/filepath"
	"testing"
)

func TestGenerateWALManagerFixture(t *testing.T) {
	if os.Getenv(regenWALFixturesEnv) == "" {
		t.Skipf("golden-fixture generator; run with %s=1 to regenerate %s/walmanager_legacy.wal", regenWALFixturesEnv, walFixturesDestDir)
	}

	dir := t.TempDir()
	basePath := filepath.Join(dir, "walmanager_legacy")
	dsn := "file:" + basePath + "?mode=wal"

	db, err := sql.Open("tinysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	execTx := func(stmts ...string) {
		t.Helper()
		tx, err := db.Begin()
		if err != nil {
			t.Fatalf("begin: %v", err)
		}
		for _, s := range stmts {
			if _, err := tx.Exec(s); err != nil {
				t.Fatalf("exec %q: %v", s, err)
			}
		}
		if err := tx.Commit(); err != nil {
			t.Fatalf("commit: %v", err)
		}
	}

	// 1. Schema. Wrapped in its own explicit transaction: this is the very
	// first write against the freshly opened, still-empty database — see
	// the package doc comment above for why that matters.
	execTx(`CREATE TABLE accounts (id INT, name TEXT, balance FLOAT64)`)

	// 2. An INSERT-only transaction (three rows, one autocommitted
	// statement => one WALManager transaction).
	if _, err := db.Exec(`INSERT INTO accounts VALUES (1, 'Alice', 100.0), (2, 'Bob', 50.0), (3, 'Carol', 75.0)`); err != nil {
		t.Fatalf("insert: %v", err)
	}

	// 3. An UPDATE-only transaction.
	if _, err := db.Exec(`UPDATE accounts SET balance = 150.0 WHERE id = 1`); err != nil {
		t.Fatalf("update: %v", err)
	}

	// 4. A DELETE-only transaction.
	if _, err := db.Exec(`DELETE FROM accounts WHERE id = 3`); err != nil {
		t.Fatalf("delete: %v", err)
	}

	// 5. A genuine multi-statement transaction: insert Dave, update Bob,
	// insert Eve, all under one BEGIN/COMMIT pair via database/sql.
	execTx(
		`INSERT INTO accounts VALUES (4, 'Dave', 10.0)`,
		`UPDATE accounts SET balance = 55.0 WHERE id = 2`,
		`INSERT INTO accounts VALUES (5, 'Eve', 20.0)`,
	)

	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	data, err := os.ReadFile(basePath + ".wal")
	if err != nil {
		t.Fatalf("read generated WAL: %v", err)
	}

	dest := filepath.Join(walFixturesDestDir, "walmanager_legacy.wal")
	if err := os.MkdirAll(filepath.Dir(dest), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(dest, data, 0o644); err != nil {
		t.Fatalf("write fixture: %v", err)
	}
	t.Logf("wrote %d bytes to %s", len(data), dest)
}

// regenWALFixturesEnv and walFixturesDestDir mirror the constants in
// internal/engine/wal_fixture_gen_test.go (separate packages can't share an
// unexported const, and importing engine here only for two literals isn't
// worth it).
const (
	regenWALFixturesEnv = "TINYSQL_REGEN_WAL_FIXTURES"
	walFixturesDestDir  = "../storage/testdata/wal_fixtures"
)
