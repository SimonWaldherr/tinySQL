// Recovery tests for the two ways a ModeWAL database used to lose or invent
// data around a checkpoint.
//
//   - Checkpointing writes the snapshot first and truncates the log second. A
//     crash in between left a log whose records were all already inside the
//     snapshot; replaying them applied every append-rows delta a second time and
//     silently duplicated committed rows. The snapshot now carries the Seq it
//     reflects, and recovery skips records at or below it.
//
//   - A damaged log tail made the database permanently unopenable, even though
//     everything before the damage had decoded and applied fine. Recovery now
//     cuts the log at the end of its last complete record and reports the
//     truncation. That also requires the truncation offset to be exact, which is
//     why countingReader implements io.ByteReader.
package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func openWALDB(t *testing.T, base string) *DB {
	t.Helper()
	db, err := OpenDB(StorageConfig{Mode: ModeWAL, Path: base})
	if err != nil {
		t.Fatalf("open %s: %v", base, err)
	}
	return db
}

// seedRows appends rows through the WAL the way an INSERT does: log the delta,
// then apply it.
func seedRows(t *testing.T, db *DB, tenant, name string, from, to int) {
	t.Helper()
	table, err := db.Get(tenant, name)
	if err != nil {
		table = NewTable(name, []Column{{Name: "id", Type: IntType}}, false)
		if err := db.Put(tenant, table); err != nil {
			t.Fatalf("put: %v", err)
		}
		table.ResetDirty()
	}
	for i := from; i < to; i++ {
		table.MarkDirtyFrom(len(table.Rows))
		table.Rows = append(table.Rows, []any{i})
		table.Version++
		if _, err := db.WAL().LogTransaction([]WALChange{{Tenant: tenant, Name: name, Table: table}}); err != nil {
			t.Fatalf("log row %d: %v", i, err)
		}
	}
}

func rowCount(t *testing.T, db *DB, tenant, name string) int {
	t.Helper()
	table, err := db.Get(tenant, name)
	if err != nil {
		return 0
	}
	return len(table.Rows)
}

// TestCheckpointCrashBeforeTruncationDoesNotDuplicateRows reproduces the crash
// window directly: save the log as it stands, let the checkpoint truncate it,
// then put the saved log back. That is byte-for-byte the state a process leaves
// behind when it dies after writing the snapshot and before truncating.
func TestCheckpointCrashBeforeTruncationDoesNotDuplicateRows(t *testing.T) {
	base := filepath.Join(t.TempDir(), "ckpt")
	walPath := base + ".wal"

	db := openWALDB(t, base)
	seedRows(t, db, "default", "accounts", 0, 25)
	if got := rowCount(t, db, "default", "accounts"); got != 25 {
		t.Fatalf("rows before checkpoint = %d, want 25", got)
	}

	// The log as it stands, all 25 rows' worth of deltas.
	preCheckpoint, err := os.ReadFile(walPath)
	if err != nil {
		t.Fatalf("read wal: %v", err)
	}
	if len(preCheckpoint) == 0 {
		t.Fatal("wal is empty before the checkpoint; the test would prove nothing")
	}

	if err := db.WAL().Checkpoint(db); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Simulate the crash: the snapshot is on disk, the log was never truncated.
	if err := os.WriteFile(walPath, preCheckpoint, 0o644); err != nil {
		t.Fatalf("restore pre-checkpoint wal: %v", err)
	}

	recovered := openWALDB(t, base)
	defer recovered.Close()
	if got := rowCount(t, recovered, "default", "accounts"); got != 25 {
		t.Errorf("rows after recovery = %d, want 25 (a higher count means the "+
			"checkpointed deltas were replayed on top of the snapshot)", got)
	}
	seen := map[int]int{}
	table, err := recovered.Get("default", "accounts")
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range table.Rows {
		if id, ok := row[0].(int); ok {
			seen[id]++
		}
	}
	for id, n := range seen {
		if n != 1 {
			t.Errorf("id %d appears %d times after recovery, want once", id, n)
		}
	}
}

// TestWritesAfterCheckpointSurviveRecovery is the other half of the watermark
// contract: rows logged after a checkpoint must NOT be skipped. Resetting Seq to
// 1 on checkpoint would make them compare below the watermark and vanish.
func TestWritesAfterCheckpointSurviveRecovery(t *testing.T) {
	base := filepath.Join(t.TempDir(), "postckpt")

	db := openWALDB(t, base)
	seedRows(t, db, "default", "accounts", 0, 10)
	if err := db.WAL().Checkpoint(db); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	seedRows(t, db, "default", "accounts", 10, 15)
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	recovered := openWALDB(t, base)
	defer recovered.Close()
	if got := rowCount(t, recovered, "default", "accounts"); got != 15 {
		t.Errorf("rows after recovery = %d, want 15 (a count of 10 means the "+
			"post-checkpoint writes were skipped as already-checkpointed)", got)
	}
}

// TestRepeatedCheckpointsKeepDataExact runs several checkpoint cycles with
// writes in between, the shape a long-running process produces.
func TestRepeatedCheckpointsKeepDataExact(t *testing.T) {
	base := filepath.Join(t.TempDir(), "repeat")

	db := openWALDB(t, base)
	total := 0
	for cycle := 0; cycle < 4; cycle++ {
		seedRows(t, db, "default", "accounts", total, total+7)
		total += 7
		if err := db.WAL().Checkpoint(db); err != nil {
			t.Fatalf("checkpoint %d: %v", cycle, err)
		}
	}
	seedRows(t, db, "default", "accounts", total, total+3)
	total += 3
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	recovered := openWALDB(t, base)
	defer recovered.Close()
	if got := rowCount(t, recovered, "default", "accounts"); got != total {
		t.Errorf("rows after recovery = %d, want %d", got, total)
	}
}

// TestGarbageWALTailStillOpens covers the tail cases that used to make the
// database unopenable. Each appends something invalid to a clean log; recovery
// must keep everything that decoded and report Truncated.
func TestGarbageWALTailStillOpens(t *testing.T) {
	cases := []struct {
		name string
		tail []byte
	}{
		{"single stray byte", []byte{0x42}},
		{"short length prefix", []byte{0xff}},
		{"plausible length, no payload", []byte{0x20, 0x01, 0x02}},
		{"zero bytes", []byte{0x00, 0x00, 0x00, 0x00}},
		{"long garbage run", []byte("this is not a gob record at all, not even close")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			base := filepath.Join(t.TempDir(), "torn")
			walPath := base + ".wal"

			db := openWALDB(t, base)
			seedRows(t, db, "default", "accounts", 0, 12)
			if err := db.Close(); err != nil {
				t.Fatalf("close: %v", err)
			}

			clean, err := os.ReadFile(walPath)
			if err != nil {
				t.Fatalf("read wal: %v", err)
			}
			if err := os.WriteFile(walPath, append(append([]byte(nil), clean...), tc.tail...), 0o644); err != nil {
				t.Fatalf("append tail: %v", err)
			}

			recovered, err := OpenDB(StorageConfig{Mode: ModeWAL, Path: base})
			if err != nil {
				t.Fatalf("database is unopenable after a damaged tail: %v", err)
			}
			defer recovered.Close()
			if got := rowCount(t, recovered, "default", "accounts"); got != 12 {
				t.Errorf("rows after recovery = %d, want 12 (the committed rows "+
					"before the damage must survive)", got)
			}
			if !recovered.HealthCheck().Recovery.Truncated {
				t.Error("Recovery.Truncated is false; a caller has no way to learn the log was cut")
			}
		})
	}
}

// TestTornWALTailIsTruncatedExactly checks that the cut lands at the end of the
// last complete record, so a second open finds a clean log and does not report
// another truncation. An offset that over-reads leaves part of the damage
// behind, which is what made the database unopenable a second time.
func TestTornWALTailIsTruncatedExactly(t *testing.T) {
	base := filepath.Join(t.TempDir(), "exact")
	walPath := base + ".wal"

	db := openWALDB(t, base)
	seedRows(t, db, "default", "accounts", 0, 8)
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	clean, err := os.ReadFile(walPath)
	if err != nil {
		t.Fatalf("read wal: %v", err)
	}
	cleanLen := len(clean)

	if err := os.WriteFile(walPath, append(append([]byte(nil), clean...), 0x7f, 0x7f, 0x7f), 0o644); err != nil {
		t.Fatalf("append tail: %v", err)
	}

	first, err := OpenDB(StorageConfig{Mode: ModeWAL, Path: base})
	if err != nil {
		t.Fatalf("first open after damage: %v", err)
	}
	if got := rowCount(t, first, "default", "accounts"); got != 8 {
		t.Errorf("rows on first recovery = %d, want 8", got)
	}
	if err := first.Close(); err != nil {
		t.Fatalf("close after first recovery: %v", err)
	}

	truncated, err := os.ReadFile(walPath)
	if err != nil {
		t.Fatalf("read truncated wal: %v", err)
	}
	if len(truncated) != cleanLen {
		t.Errorf("wal is %d bytes after truncation, want the clean length %d: %s",
			len(truncated), cleanLen,
			fmt.Sprintf("a shorter file dropped a good record, a longer one left damage behind"))
	}

	second, err := OpenDB(StorageConfig{Mode: ModeWAL, Path: base})
	if err != nil {
		t.Fatalf("second open, after the log was repaired: %v", err)
	}
	defer second.Close()
	if got := rowCount(t, second, "default", "accounts"); got != 8 {
		t.Errorf("rows on second recovery = %d, want 8", got)
	}
	if second.HealthCheck().Recovery.Truncated {
		t.Error("second open still reports Truncated; the first truncation did not fully repair the log")
	}
}
