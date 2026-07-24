// Golden WAL fixture generator — AdvancedWAL side.
//
// This is Stage 0 of the WAL-consolidation characterization effort (see
// internal/storage/wal_fixture_test.go for the replay-side verification and
// internal/storage/wal_crash_harness_test.go for crash simulation built on
// top of it). It is a one-shot generator, not part of the normal test
// suite — run it explicitly to (re)produce
// internal/storage/testdata/wal_fixtures/advancedwal_legacy.wal:
//
//	TINYSQL_REGEN_WAL_FIXTURES=1 go test ./internal/engine/ -run TestGenerateAdvancedWALFixture -v
//
// Left unguarded this would silently rewrite the checked-in golden file
// every time `go test ./...` runs: its bytes embed wall-clock timestamps
// (WALRecord.Timestamp) and are therefore never byte-stable across runs,
// which is not what a "golden" characterization fixture should do.
//
// The single-statement pieces (CREATE TABLE / INSERT / UPDATE / DELETE)
// below are driven through the real production entry point, engine.Execute,
// exactly as a real ModeAdvancedWAL caller would (see wal_logging.go and
// wal_logging_test.go in this package): AdvancedWAL logging is fully
// automatic there, one implicit transaction per top-level statement.
//
// There is deliberately no engine-level path that bundles several
// top-level statements into one AdvancedWAL transaction: an explicit
// database/sql transaction (BEGIN/COMMIT) against ModeAdvancedWAL does not
// route to AdvancedWAL at all, because storage.DB.SnapshotForTx's shadow
// clone copies only the unrelated basic WALManager pointer, never
// advancedWAL (see internal/storage/db.go's SnapshotForTx) — a real,
// separate gap worth flagging for the consolidation work, not something to
// paper over here. So this generator drives the fixture's "multi-statement
// transaction" scenario directly through AdvancedWAL's own public Log* API
// (LogBegin/LogInsert/LogUpdate/LogCommit) — exactly the methods a real
// multi-op transaction would call, just orchestrated by hand instead of by
// a SQL/driver layer that doesn't support it yet.
package engine

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// regenWALFixturesEnv gates both golden-fixture generators (this file and
// internal/driver's WALManager counterpart) behind an explicit opt-in.
const regenWALFixturesEnv = "TINYSQL_REGEN_WAL_FIXTURES"

// walFixturesDestDir is where both generators (this package and
// internal/driver) deposit their output, relative to their own package
// directory — both are direct siblings of internal/storage.
const walFixturesDestDir = "../storage/testdata/wal_fixtures"

func TestGenerateAdvancedWALFixture(t *testing.T) {
	if os.Getenv(regenWALFixturesEnv) == "" {
		t.Skipf("golden-fixture generator; run with %s=1 to regenerate %s/advancedwal_legacy.wal", regenWALFixturesEnv, walFixturesDestDir)
	}

	dir := t.TempDir()
	walPath := filepath.Join(dir, "advancedwal_legacy")

	wal, err := storage.OpenAdvancedWAL(storage.AdvancedWALConfig{Path: walPath})
	if err != nil {
		t.Fatalf("open advanced WAL: %v", err)
	}
	db := storage.NewDB()
	db.AttachAdvancedWAL(wal)

	exec := func(sql string) {
		t.Helper()
		stmt, err := NewParser(sql).ParseStatement()
		if err != nil {
			t.Fatalf("parse %q: %v", sql, err)
		}
		if _, err := Execute(context.Background(), db, "default", stmt); err != nil {
			t.Fatalf("exec %q: %v", sql, err)
		}
	}

	// 1. Schema, then an INSERT-only transaction (three rows in one
	// autocommitted statement => one AdvancedWAL transaction).
	exec(`CREATE TABLE accounts (id INT, name TEXT, balance FLOAT64)`)
	exec(`INSERT INTO accounts VALUES (1, 'Alice', 100.0), (2, 'Bob', 50.0), (3, 'Carol', 75.0)`)

	// 2. An UPDATE-only transaction.
	exec(`UPDATE accounts SET balance = 150.0 WHERE id = 1`)

	// 3. A DELETE-only transaction.
	exec(`DELETE FROM accounts WHERE id = 3`)

	// Table state is now: [1 Alice 150.0], [2 Bob 50.0] (Carol deleted).

	// 4. A genuine multi-statement transaction: insert Dave, update Bob,
	// insert Eve, all under one BEGIN/COMMIT pair. Driven directly through
	// AdvancedWAL's own Log* API — see the package doc comment above for
	// why engine.Execute alone cannot produce this shape today.
	cols := []storage.Column{
		{Name: "id", Type: storage.IntType},
		{Name: "name", Type: storage.TextType},
		{Name: "balance", Type: storage.Float64Type},
	}
	txID := wal.NewAutoTxID()
	if _, err := wal.LogBegin(txID); err != nil {
		t.Fatalf("LogBegin: %v", err)
	}
	if _, err := wal.LogInsert(txID, "default", "accounts", 2, []any{4, "Dave", 10.0}, cols); err != nil {
		t.Fatalf("LogInsert Dave: %v", err)
	}
	if _, err := wal.LogUpdate(txID, "default", "accounts", 1, []any{2, "Bob", 50.0}, []any{2, "Bob", 55.0}, cols); err != nil {
		t.Fatalf("LogUpdate Bob: %v", err)
	}
	if _, err := wal.LogInsert(txID, "default", "accounts", 3, []any{5, "Eve", 20.0}, cols); err != nil {
		t.Fatalf("LogInsert Eve: %v", err)
	}
	if _, err := wal.LogCommit(txID); err != nil {
		t.Fatalf("LogCommit: %v", err)
	}

	if err := wal.Close(); err != nil {
		t.Fatalf("close WAL: %v", err)
	}

	data, err := os.ReadFile(walPath)
	if err != nil {
		t.Fatalf("read generated WAL: %v", err)
	}

	dest := filepath.Join(walFixturesDestDir, "advancedwal_legacy.wal")
	if err := os.MkdirAll(filepath.Dir(dest), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(dest, data, 0o644); err != nil {
		t.Fatalf("write fixture: %v", err)
	}
	t.Logf("wrote %d bytes to %s", len(data), dest)
}
