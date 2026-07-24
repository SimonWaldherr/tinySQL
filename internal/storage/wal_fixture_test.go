// Golden WAL fixture replay tests — Stage 0 of the WAL-consolidation
// characterization effort (see internal/storage/wal_crash_harness_test.go
// for the crash-simulation harness built on top of this file, and
// internal/engine/wal_fixture_gen_test.go / internal/driver/wal_fixture_gen_test.go
// for the generators that produced testdata/wal_fixtures/*.wal).
//
// Both fixtures encode the same logical history against a table named
// "accounts" (tenant "default", columns id INT, name TEXT, balance
// FLOAT64), each committed transaction driven through the real production
// entry point for its WAL implementation (database/sql + a mode=wal DSN for
// WALManager; engine.Execute + AdvancedWAL's own Log* API for AdvancedWAL —
// see the generators' doc comments for exactly which parts went through
// which real API and why):
//
//  1. CREATE TABLE accounts (...)
//  2. INSERT-only transaction: (1,Alice,100.0), (2,Bob,50.0), (3,Carol,75.0)
//  3. UPDATE-only transaction: balance=150.0 WHERE id=1 (Alice)
//  4. DELETE-only transaction: WHERE id=3 (Carol)
//  5. A multi-statement transaction: insert Dave(4,10.0), update Bob's
//     balance to 55.0, insert Eve(5,20.0) — all one commit.
//
// Final expected state: Alice(150.0), Bob(55.0), Dave(10.0), Eve(20.0).
//
// Two characterization findings surfaced while building these fixtures,
// both real and both out of scope to fix under Stage 0 (db.go and
// wal_advanced.go are frozen for this stage):
//
//   - WALManager has NO per-record checksum today. walRecord (db.go) carries
//     no CRC/hash field, and replayWAL never verifies one. A single flipped
//     byte inside an already-complete gob record's payload is, in the
//     general case, silently misapplied or silently produces a different
//     (but structurally valid) value — gob framing corruption is usually
//     caught as a decode error (see replayWAL's io.ErrUnexpectedEOF/
//     io.ErrNoProgress handling), but corruption confined to a payload byte
//     is not. This is the exact gap the later consolidation work is meant
//     to close by giving ModeWAL a checksum for the first time. AdvancedWAL,
//     by contrast, already has a real CRC32-Castagnoli checksum over every
//     field including row images (see wal_advanced.go's calculateChecksum)
//     plus a legacy-checksum fallback for old files, and Recover stops
//     cleanly the instant either check fails. See
//     TestWALManagerBitFlipCorruptionIsNotDetected and
//     TestAdvancedWALBitFlipCorruptionIsDetected in wal_crash_harness_test.go.
//
//   - A more severe, unrelated pair of findings in the driver/storage
//     boundary (both confirmed empirically while building the WALManager
//     fixture — see internal/driver/wal_fixture_gen_test.go's doc comment):
//
//     First, storage.DB.ShallowCloneForTable special-cases an empty
//     db.tenants map by returning a bare NewDB() that does NOT copy the
//     .wal field. internal/driver's conn.execStatement uses
//     ShallowCloneForTable for every non-transaction (autocommit) write, so
//     the very first autocommit write against a brand-new, empty ModeWAL
//     database permanently detaches WALManager from that live *storage.DB
//     for the rest of the process's life — silently, with no error, and
//     with HealthCheck() still reporting WALActive: true throughout, since
//     it reads db.wal off whatever *storage.DB happens to be current, and
//     that field is simply never populated on the replacement shadow.
//
//     Second — and this one is not specific to explicit transactions at
//     all — every write through the driver double-logs to the WAL file
//     today. internal/engine.Execute (== executeStatement) unconditionally
//     calls maybeLogToWALManager once for every atomic-DML statement
//     (INSERT/UPDATE/DELETE) against whatever *storage.DB it's given. The
//     driver's autocommit path passes it a ShallowCloneForTable shadow and
//     then, right after Execute returns, independently computes
//     CollectWALChanges(base, shadow) and calls wal.LogTransaction itself —
//     so a single autocommit statement produces *two* on-disk WAL
//     transactions, not one. An explicit BEGIN/COMMIT transaction compounds
//     this further: each statement inside it runs via plain
//     engine.Execute(ctx, c.shadow, ...), so maybeLogToWALManager fires per
//     statement (immediately, against the real shared WAL file) in addition
//     to commitTx's own bundled diff-and-log at COMMIT. The double-logged
//     bytes are harmless to final-state correctness on their own (both log
//     calls diff the same before/after pair, so re-applying either is
//     idempotent), but the explicit-transaction case has a real atomicity
//     consequence beyond wasted space: each mid-transaction statement's
//     diff is already durably recorded as its own *committed* WAL
//     transaction the instant it runs, before the enclosing SQL transaction
//     actually commits — so a transaction that is later ROLLED BACK, not
//     committed, still leaves its intermediate per-statement effects
//     recorded as committed in the on-disk WAL if the process crashes
//     before the rollback's (in-memory-only) discard takes effect.
//
//     Both findings live at the internal/driver <-> internal/storage
//     boundary (root cause for the first is in db.go's
//     ShallowCloneForTable, which this stage must not modify); this
//     package's fixture generator works around the first by wrapping its
//     very first write in an explicit transaction (see
//     internal/driver/wal_fixture_gen_test.go) and simply documents the
//     second, since redundant idempotent re-application doesn't affect the
//     final state this test asserts (11 WALManager transactions get
//     recovered from a fixture whose SQL-level history is only 5
//     statement groups — see TestWALManagerFixtureReplaysCorrectly's log
//     output). Both are genuinely useful input for the WAL consolidation
//     stages and are flagged here rather than silently worked around.
package storage

import (
	"os"
	"path/filepath"
	"testing"
)

// walFixturesDir holds the checked-in golden WAL sidecar files this test
// (and wal_crash_harness_test.go) replays.
const walFixturesDir = "testdata/wal_fixtures"

// copyFixture copies a checked-in golden fixture into a scratch directory
// under destName, returning the copy's path. Recovery opens its WAL file
// read-write and can truncate a torn tail (see replayWAL/OpenWAL), so tests
// must never operate directly on the checked-in golden file.
func copyFixture(t *testing.T, srcName, destDir, destName string) string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(walFixturesDir, srcName))
	if err != nil {
		t.Fatalf("read fixture %s: %v", srcName, err)
	}
	dest := filepath.Join(destDir, destName)
	if err := os.WriteFile(dest, data, 0o644); err != nil {
		t.Fatalf("write fixture copy: %v", err)
	}
	return dest
}

// accountsFinalState is the expected end state shared by both fixtures
// (see the package doc comment above for the transaction history that
// produces it), keyed by id since row order is not part of either WAL
// implementation's contract.
type accountRow struct {
	name    string
	balance float64
}

func expectedAccountsFinalState() map[int]accountRow {
	return map[int]accountRow{
		1: {"Alice", 150.0},
		2: {"Bob", 55.0},
		4: {"Dave", 10.0},
		5: {"Eve", 20.0},
	}
}

// assertAccountsFinalState checks rows (as recovered from a WAL replay)
// against expectedAccountsFinalState, tolerating any row order.
func assertAccountsFinalState(t *testing.T, rows [][]any) {
	t.Helper()
	want := expectedAccountsFinalState()
	if len(rows) != len(want) {
		t.Fatalf("recovered %d rows, want %d: %+v", len(rows), len(want), rows)
	}
	seen := make(map[int]bool, len(rows))
	for _, row := range rows {
		if len(row) != 3 {
			t.Fatalf("row has %d cells, want 3: %+v", len(row), row)
		}
		id, ok := row[0].(int)
		if !ok {
			t.Fatalf("row id cell is %T, want int: %+v", row[0], row)
		}
		wantRow, ok := want[id]
		if !ok {
			t.Fatalf("unexpected recovered id %d: %+v", id, row)
		}
		if seen[id] {
			t.Fatalf("duplicate recovered id %d", id)
		}
		seen[id] = true
		name, _ := row[1].(string)
		if name != wantRow.name {
			t.Errorf("id %d: name = %q, want %q", id, name, wantRow.name)
		}
		balance, _ := row[2].(float64)
		if balance != wantRow.balance {
			t.Errorf("id %d: balance = %v, want %v", id, balance, wantRow.balance)
		}
	}
}

// TestWALManagerFixtureReplaysCorrectly opens walmanager_legacy.wal through
// the real replay-on-open path (OpenDB(ModeWAL, ...) -> OpenWAL ->
// replayWAL) against a fresh DB and checks the recovered "accounts" table
// matches the fixture's known transaction history exactly.
func TestWALManagerFixtureReplaysCorrectly(t *testing.T) {
	dir := t.TempDir()
	copyFixture(t, "walmanager_legacy.wal", dir, "walmanager_legacy.wal")
	// OpenWAL appends ".wal" to cfg.Path itself, so the config path must be
	// the copy's name minus that suffix.
	base := filepath.Join(dir, "walmanager_legacy")

	db, err := OpenDB(StorageConfig{Mode: ModeWAL, Path: base})
	if err != nil {
		t.Fatalf("OpenDB: %v", err)
	}
	defer func() { _ = db.Close() }()

	health := db.HealthCheck()
	if health.Recovery.Truncated {
		t.Fatalf("replay reported Truncated=true against a known-good fixture")
	}
	if health.Recovery.RecoveredTransactions == 0 {
		t.Fatalf("replay recovered zero transactions; fixture may be empty or unreadable")
	}
	t.Logf("recovered %d WALManager transactions", health.Recovery.RecoveredTransactions)

	tbl, err := db.Get("default", "accounts")
	if err != nil {
		t.Fatalf("get accounts: %v", err)
	}
	assertAccountsFinalState(t, tbl.Rows)
}

// TestAdvancedWALFixtureReplaysCorrectly opens advancedwal_legacy.wal
// through the real recovery path (OpenDB(ModeAdvancedWAL, ...) ->
// OpenAdvancedWAL -> Recover) against a fresh DB and checks the recovered
// "accounts" table matches the fixture's known transaction history exactly.
func TestAdvancedWALFixtureReplaysCorrectly(t *testing.T) {
	dir := t.TempDir()
	path := copyFixture(t, "advancedwal_legacy.wal", dir, "advancedwal_legacy.wal")

	db, err := OpenDB(StorageConfig{Mode: ModeAdvancedWAL, Path: path})
	if err != nil {
		t.Fatalf("OpenDB: %v", err)
	}
	defer func() { _ = db.Close() }()

	health := db.HealthCheck()
	if health.Recovery.RecoveredOperations == 0 {
		t.Fatalf("replay recovered zero operations; fixture may be empty or unreadable")
	}
	t.Logf("recovered %d AdvancedWAL operations", health.Recovery.RecoveredOperations)

	tbl, err := db.Get("default", "accounts")
	if err != nil {
		t.Fatalf("get accounts: %v", err)
	}
	assertAccountsFinalState(t, tbl.Rows)
}
