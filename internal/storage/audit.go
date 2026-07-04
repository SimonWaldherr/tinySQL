// Package storage: tamper-evident audit log.
//
// What: an append-only log of every statement Execute runs (see
// AttachAuditLog/DB.AuditLog and internal/engine/audit.go for the hook),
// where each entry's hash covers its own fields plus the previous entry's
// hash — a hash chain, the same integrity primitive used by blockchains
// and git commit history. Altering, reordering, or deleting any entry
// breaks the chain from that point forward in a way Verify detects.
//
// Why this matters for the intended use cases (industry compliance,
// scientific reproducibility, security-sensitive deployments): a plain log
// file can be edited by anyone with filesystem access, silently. A hash
// chain doesn't prevent that access, but it makes silent editing
// detectable — Verify recomputes every hash and reports exactly where the
// chain first diverges from what's recorded, which is what audit/compliance
// tooling actually needs (proof of tampering, not prevention of it — true
// tamper-*prevention* needs signing with a key the log's own host doesn't
// hold, which is out of scope for an embedded database).
package storage

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strconv"
	"sync"
	"time"
)

// AuditEntry is one recorded statement execution.
type AuditEntry struct {
	Seq       uint64
	Timestamp time.Time
	Tenant    string
	User      string // empty if RBAC isn't enabled / no user was set
	Statement string // original SQL text if available, else a best-effort fallback
	Success   bool
	Error     string // populated only when !Success
	PrevHash  string // hex-encoded SHA-256 of the previous entry, "" for the first entry
	Hash      string // hex-encoded SHA-256 of this entry's own fields + PrevHash
}

// computeHash derives an entry's hash deterministically from its fields and
// PrevHash. Seq is included so two otherwise-identical statements run back
// to back (same text, same instant down to the timestamp's resolution)
// still produce different hashes and can't be silently swapped for one
// another.
func (e AuditEntry) computeHash() string {
	h := sha256.New()
	h.Write([]byte(e.PrevHash))
	h.Write([]byte(strconv.FormatUint(e.Seq, 10)))
	h.Write([]byte(e.Timestamp.UTC().Format(time.RFC3339Nano)))
	h.Write([]byte(e.Tenant))
	h.Write([]byte(e.User))
	h.Write([]byte(e.Statement))
	if e.Success {
		h.Write([]byte{1})
	} else {
		h.Write([]byte{0})
	}
	h.Write([]byte(e.Error))
	return hex.EncodeToString(h.Sum(nil))
}

// AuditLog is an in-memory, optionally file-backed hash-chained log. Safe
// for concurrent use.
type AuditLog struct {
	mu       sync.Mutex
	entries  []AuditEntry
	lastHash string
	nextSeq  uint64
	file     *os.File // nil if in-memory only
	writer   *bufio.Writer
}

// NewAuditLog creates an in-memory-only audit log (not persisted to disk —
// useful for tests, or callers that only care about the current process's
// lifetime).
func NewAuditLog() *AuditLog {
	return &AuditLog{}
}

// OpenAuditLog opens (creating if necessary) an append-only JSONL file —
// one JSON-encoded AuditEntry per line, chosen over a binary format for
// the same reason ModeJSON exists: human-readable, diffable, greppable,
// and inspectable with ordinary tools during an incident, at the cost of
// some size on disk. If the file already has entries, they're replayed and
// the chain is verified as part of opening — a corrupted or tampered
// existing log is caught immediately at startup rather than silently
// trusted.
func OpenAuditLog(path string) (*AuditLog, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open audit log: %w", err)
	}
	a := &AuditLog{file: f}

	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		var e AuditEntry
		if err := json.Unmarshal(line, &e); err != nil {
			f.Close()
			return nil, fmt.Errorf("audit log %s: corrupt entry at line %d: %w", path, len(a.entries)+1, err)
		}
		a.entries = append(a.entries, e)
	}
	if err := scanner.Err(); err != nil {
		f.Close()
		return nil, fmt.Errorf("audit log %s: read: %w", path, err)
	}
	if err := a.verifyLocked(); err != nil {
		f.Close()
		return nil, fmt.Errorf("audit log %s: %w (log may have been tampered with)", path, err)
	}
	if n := len(a.entries); n > 0 {
		a.lastHash = a.entries[n-1].Hash
		a.nextSeq = a.entries[n-1].Seq + 1
	}
	// Position for appending new entries; O_APPEND already ensures writes
	// land at EOF regardless of this seek, but Seek keeps f's offset
	// consistent with an append-only mental model.
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		f.Close()
		return nil, fmt.Errorf("audit log %s: seek: %w", path, err)
	}
	a.writer = bufio.NewWriter(f)
	return a, nil
}

// Append records one statement execution and returns the entry, hash chain
// already computed. statement should be the original SQL text when
// available; user may be empty (RBAC not enabled, or no user in context).
func (a *AuditLog) Append(tenant, user, statement string, success bool, errMsg string) AuditEntry {
	a.mu.Lock()
	defer a.mu.Unlock()

	e := AuditEntry{
		Seq:       a.nextSeq,
		Timestamp: time.Now(),
		Tenant:    tenant,
		User:      user,
		Statement: statement,
		Success:   success,
		Error:     errMsg,
		PrevHash:  a.lastHash,
	}
	e.Hash = e.computeHash()

	a.entries = append(a.entries, e)
	a.lastHash = e.Hash
	a.nextSeq++

	if a.writer != nil {
		if b, err := json.Marshal(e); err == nil {
			a.writer.Write(b)
			a.writer.WriteByte('\n')
			a.writer.Flush()
		}
	}
	return e
}

// Verify walks the whole chain and recomputes every hash, returning an
// error identifying the first entry (by Seq) where the recorded hash
// doesn't match — either the entry's own fields were altered, or PrevHash
// no longer matches its predecessor (an entry was inserted, removed, or
// reordered).
func (a *AuditLog) Verify() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.verifyLocked()
}

func (a *AuditLog) verifyLocked() error {
	prevHash := ""
	for i, e := range a.entries {
		if e.PrevHash != prevHash {
			return fmt.Errorf("chain broken at entry seq=%d (index %d): PrevHash %q does not match preceding entry's hash %q", e.Seq, i, e.PrevHash, prevHash)
		}
		want := e.computeHash()
		if e.Hash != want {
			return fmt.Errorf("hash mismatch at entry seq=%d (index %d): stored %q, recomputed %q — entry contents were modified after being logged", e.Seq, i, e.Hash, want)
		}
		prevHash = e.Hash
	}
	return nil
}

// Entries returns a copy of every logged entry, in order.
func (a *AuditLog) Entries() []AuditEntry {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := make([]AuditEntry, len(a.entries))
	copy(out, a.entries)
	return out
}

// Close flushes and closes the backing file, if any. A no-op for an
// in-memory-only log (NewAuditLog).
func (a *AuditLog) Close() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.writer != nil {
		if err := a.writer.Flush(); err != nil {
			return err
		}
	}
	if a.file != nil {
		return a.file.Close()
	}
	return nil
}
