// Tests for the tamper-evident audit log (audit.go).
package storage

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestAuditLogAppendAndVerify(t *testing.T) {
	log := NewAuditLog()
	log.Append("default", "alice", "SELECT * FROM t", true, "")
	log.Append("default", "alice", "INSERT INTO t VALUES (1)", true, "")
	log.Append("default", "bob", "DELETE FROM t", false, "access denied")

	entries := log.Entries()
	if len(entries) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(entries))
	}
	for i, e := range entries {
		if int(e.Seq) != i {
			t.Errorf("entry %d: expected Seq=%d, got %d", i, i, e.Seq)
		}
	}
	if entries[0].PrevHash != "" {
		t.Errorf("expected first entry's PrevHash to be empty, got %q", entries[0].PrevHash)
	}
	if entries[1].PrevHash != entries[0].Hash {
		t.Errorf("expected entry 1's PrevHash to equal entry 0's Hash")
	}
	if entries[2].Success {
		t.Error("expected entry 2 (denied DELETE) to have Success=false")
	}
	if entries[2].Error != "access denied" {
		t.Errorf("expected entry 2's Error to be recorded, got %q", entries[2].Error)
	}

	if err := log.Verify(); err != nil {
		t.Errorf("expected a freshly-appended, untouched log to verify cleanly: %v", err)
	}
}

func TestAuditLogDetectsFieldTampering(t *testing.T) {
	log := NewAuditLog()
	log.Append("default", "alice", "SELECT * FROM t", true, "")
	log.Append("default", "alice", "DROP TABLE audit_log", true, "")

	// Simulate an attacker editing an entry's statement text in memory
	// (equivalent to hand-editing a line of the on-disk JSONL file) without
	// recomputing the hash — the realistic tampering scenario Verify exists
	// to catch.
	log.entries[1].Statement = "SELECT 1" // hide what really happened

	err := log.Verify()
	if err == nil {
		t.Fatal("expected Verify to detect the tampered entry, got nil error")
	}
	if !strings.Contains(err.Error(), "seq=1") {
		t.Errorf("expected error to identify the tampered entry (seq=1), got: %v", err)
	}
}

func TestAuditLogDetectsReordering(t *testing.T) {
	log := NewAuditLog()
	log.Append("default", "alice", "stmt A", true, "")
	log.Append("default", "alice", "stmt B", true, "")
	log.Append("default", "alice", "stmt C", true, "")

	// Simulate an attacker deleting the middle entry to hide it — this
	// breaks the chain because entry 2's PrevHash no longer matches entry
	// 0's Hash.
	log.entries = []AuditEntry{log.entries[0], log.entries[2]}

	if err := log.Verify(); err == nil {
		t.Fatal("expected Verify to detect a deleted/reordered entry, got nil error")
	}
}

func TestAuditLogFilePersistenceAndReplay(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "audit.jsonl")

	log, err := OpenAuditLog(path)
	if err != nil {
		t.Fatalf("OpenAuditLog: %v", err)
	}
	log.Append("default", "alice", "CREATE TABLE t (id INT)", true, "")
	log.Append("default", "alice", "INSERT INTO t VALUES (1)", true, "")
	if err := log.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Reopen: entries must survive and the chain must still verify.
	log2, err := OpenAuditLog(path)
	if err != nil {
		t.Fatalf("reopen OpenAuditLog: %v", err)
	}
	defer log2.Close()
	entries := log2.Entries()
	if len(entries) != 2 {
		t.Fatalf("expected 2 entries after reopen, got %d", len(entries))
	}
	if entries[1].Statement != "INSERT INTO t VALUES (1)" {
		t.Errorf("unexpected entry after reopen: %+v", entries[1])
	}

	// Appending after reopen must continue the chain correctly (not reset
	// PrevHash to empty).
	log2.Append("default", "alice", "DELETE FROM t", true, "")
	entries = log2.Entries()
	if entries[2].PrevHash != entries[1].Hash {
		t.Error("expected the chain to continue correctly across a reopen")
	}
	if err := log2.Verify(); err != nil {
		t.Errorf("expected the log to verify after reopen + append: %v", err)
	}
}

func TestAuditLogFileTamperingDetectedOnReopen(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "audit.jsonl")

	log, err := OpenAuditLog(path)
	if err != nil {
		t.Fatalf("OpenAuditLog: %v", err)
	}
	log.Append("default", "alice", "SELECT 1", true, "")
	log.Append("default", "alice", "DROP TABLE important_evidence", true, "")
	if err := log.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Simulate an attacker hand-editing the on-disk file to remove evidence
	// of the second statement, by truncating the file to just the first line.
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	lines := strings.SplitN(string(data), "\n", 2)
	if err := os.WriteFile(path, []byte(lines[0]+"\n"), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	// This truncation actually produces a *valid* shorter chain (the first
	// entry alone still verifies — you can't detect "an entry used to
	// exist after this one" from a hash chain alone, only "this entry's
	// content or position was altered"). Confirm that expected limitation
	// directly rather than asserting something the design can't promise.
	log2, err := OpenAuditLog(path)
	if err != nil {
		t.Fatalf("expected a validly-truncated log to still open (truncation to a valid prefix is not detectable): %v", err)
	}
	defer log2.Close()
	if len(log2.Entries()) != 1 {
		t.Fatalf("expected 1 entry after truncation, got %d", len(log2.Entries()))
	}

	// Now corrupt the remaining entry's content in place (a realistic
	// tampering attempt: editing what's left instead of just deleting a
	// suffix) — this *is* detectable and must be rejected at open time.
	tamperedLine := strings.Replace(lines[0], `"SELECT 1"`, `"SELECT 2"`, 1)
	if err := os.WriteFile(path, []byte(tamperedLine+"\n"), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	if _, err := OpenAuditLog(path); err == nil {
		t.Fatal("expected OpenAuditLog to reject a file with a tampered entry")
	}
}
