// Crash-simulation harness — Stage 0 of the WAL-consolidation
// characterization effort. Built on top of the golden fixtures verified in
// wal_fixture_test.go (read that file's package doc comment first for the
// fixtures' transaction history and the characterization findings already
// surfaced while building them).
//
// The core idea: rather than hand-deriving "the expected table contents
// after transaction N" for every truncation point (fragile, and it would
// really just be re-deriving the SQL semantics by hand), this harness
// exploits a self-referential invariant that holds regardless of what the
// transactions actually contain:
//
//	replaying any truncated/corrupted prefix must produce EXACTLY the same
//	result as replaying the fixture cleanly truncated at the end of the
//	last fully-committed transaction that fits inside that prefix.
//
// That "reference" replay is produced by the exact same OpenDB/OpenAdvancedWAL
// recovery path being tested, just fed a shorter — but always cleanly
// transaction-aligned — prefix. Any divergence means a transaction was
// partially applied, which is the one outcome the task treats as an
// unconditional bug.
package storage

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

// ─────────────────────────── record-boundary scanning ───────────────────────

// recordBoundary is one successfully-decoded record's end offset within a
// raw WAL byte stream, and whether that record is a transaction-commit
// marker (a point at which the replayed state contains only whole
// transactions, never a partial one).
type recordBoundary struct {
	offset   int64
	isCommit bool
}

// countCompleteRecords decodes as many complete records as possible from
// data using decodeNext (which decodes exactly one record from dec and
// returns its decode error, if any) and returns how many succeeded before
// the first error (including a clean EOF).
func countCompleteRecords(data []byte, decodeNext func(*gob.Decoder) error) int {
	dec := gob.NewDecoder(bytes.NewReader(data))
	n := 0
	for {
		if err := decodeNext(dec); err != nil {
			return n
		}
		n++
	}
}

// findRecordBoundary binary-searches the minimal prefix length of data that
// contains at least k complete, decodable records.
//
// This cannot be done with a single running byte counter on the shared
// io.Reader (e.g. wrapping it in a countingReader and reading cr.n after
// each Decode call): gob.Decoder pulls data from its underlying io.Reader
// in its own internal chunks, not one record at a time, so for a small
// in-memory fixture a single physical Read can — and does — hand the
// decoder the entire remaining file in one shot on the very first Decode
// call, making a running byte counter jump straight to EOF regardless of
// which record is "current". Binary search sidesteps this entirely: each
// trial re-decodes data[:mid] from scratch with a fresh Decoder and simply
// counts how many complete records that exact byte slice contains.
func findRecordBoundary(data []byte, k int, decodeNext func(*gob.Decoder) error) int64 {
	lo, hi := 0, len(data)
	for lo < hi {
		mid := (lo + hi) / 2
		if countCompleteRecords(data[:mid], decodeNext) >= k {
			hi = mid
		} else {
			lo = mid + 1
		}
	}
	return int64(lo)
}

// walManagerRecordBoundaries decodes data as a stream of walRecord values
// (WALManager's on-disk format; see db.go) and returns, for each record
// that decodes cleanly (in order), the minimal byte offset immediately
// after it, along with whether it is a commit marker.
func walManagerRecordBoundaries(data []byte) []recordBoundary {
	decodeOne := func(dec *gob.Decoder) error {
		var rec walRecord
		return dec.Decode(&rec)
	}
	// First pass: linear scan for record count and each record's type, in
	// order — this part doesn't need byte offsets, just successive Decode
	// calls on one shared Decoder.
	dec := gob.NewDecoder(bytes.NewReader(data))
	var kinds []bool // isCommit, per record index
	for {
		var rec walRecord
		if err := dec.Decode(&rec); err != nil {
			break
		}
		kinds = append(kinds, rec.Type == walRecordCommit)
	}
	out := make([]recordBoundary, len(kinds))
	for i := range kinds {
		out[i] = recordBoundary{offset: findRecordBoundary(data, i+1, decodeOne), isCommit: kinds[i]}
	}
	return out
}

// advancedWALRecordBoundaries is walManagerRecordBoundaries' counterpart for
// AdvancedWAL's on-disk format (WALRecord; see wal_advanced.go).
func advancedWALRecordBoundaries(data []byte) []recordBoundary {
	decodeOne := func(dec *gob.Decoder) error {
		var rec WALRecord
		return dec.Decode(&rec)
	}
	dec := gob.NewDecoder(bytes.NewReader(data))
	var kinds []bool
	for {
		var rec WALRecord
		if err := dec.Decode(&rec); err != nil {
			break
		}
		kinds = append(kinds, rec.OpType == WALOpCommit)
	}
	out := make([]recordBoundary, len(kinds))
	for i := range kinds {
		out[i] = recordBoundary{offset: findRecordBoundary(data, i+1, decodeOne), isCommit: kinds[i]}
	}
	return out
}

// truncationCandidates returns every record boundary in boundaries, plus a
// handful of interior byte offsets within each inter-boundary span (not
// literally every byte — the task asks for "a handful of offsets per
// record" to keep this running in reasonable time).
func truncationCandidates(boundaries []recordBoundary) []int64 {
	var out []int64
	prev := int64(0)
	for _, b := range boundaries {
		span := b.offset - prev
		if span > 1 {
			out = append(out, prev+1)
		}
		if span > 3 {
			out = append(out, prev+span/2)
		}
		if span > 2 {
			out = append(out, b.offset-1)
		}
		out = append(out, b.offset)
		prev = b.offset
	}
	return out
}

// ─────────────────────────── prefix replay helpers ───────────────────────────

// replayWALManagerPrefix writes data as a WAL sidecar and opens it through
// the real ModeWAL recovery path (OpenDB -> OpenWAL -> replayWAL) against a
// fresh DB, returning the recovered "accounts" rows (nil if the table never
// got created within this prefix) and the reported RecoveryStatus.
//
// Unlike a pure truncation (which replayWAL always turns into a clean
// Truncated=true result via its io.ErrUnexpectedEOF/io.ErrNoProgress
// handling), a bit-flip can produce a gob decode error of some other kind
// (e.g. "bad data: field numbers out of bounds"), which replayWAL treats as
// a hard failure and propagates out of OpenWAL/OpenDB rather than
// truncating. That's still a safe outcome for the "never partially
// applied" invariant — decoding never even completed, so nothing from that
// record onward was applied — so this returns the error to the caller
// instead of failing the test outright; TestWALManagerCrashTruncationNeverPartiallyApplies
// (pure truncation only) treats any such error as a genuine failure, while
// the bit-flip test treats it as one of the two acceptable outcomes.
func replayWALManagerPrefix(t *testing.T, data []byte) ([][]any, RecoveryStatus, error) {
	t.Helper()
	dir := t.TempDir()
	base := filepath.Join(dir, "wm")
	if err := os.WriteFile(base+".wal", data, 0o644); err != nil {
		t.Fatalf("write prefix: %v", err)
	}
	db, err := OpenDB(StorageConfig{Mode: ModeWAL, Path: base})
	if err != nil {
		return nil, RecoveryStatus{}, err
	}
	defer func() { _ = db.Close() }()
	health := db.HealthCheck()
	tbl, tblErr := db.Get("default", "accounts")
	if tblErr != nil {
		return nil, health.Recovery, nil
	}
	return tbl.Rows, health.Recovery, nil
}

// replayAdvancedWALPrefix is replayWALManagerPrefix's counterpart for
// ModeAdvancedWAL (OpenAdvancedWAL -> Recover).
func replayAdvancedWALPrefix(t *testing.T, data []byte) [][]any {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "adv.wal")
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("write prefix: %v", err)
	}
	wal, err := OpenAdvancedWAL(AdvancedWALConfig{Path: path})
	if err != nil {
		t.Fatalf("OpenAdvancedWAL on prefix (len %d): %v", len(data), err)
	}
	defer func() { _ = wal.Close() }()
	db := NewDB()
	if _, err := wal.Recover(db); err != nil {
		t.Fatalf("Recover on prefix (len %d): %v", len(data), err)
	}
	tbl, err := db.Get("default", "accounts")
	if err != nil {
		return nil
	}
	return tbl.Rows
}

// ─────────────────────────── the atomicity invariant itself ─────────────────

// referenceStates precomputes, for a fixture's raw bytes, the recovered row
// state at every transaction-commit boundary (plus the empty-prefix base
// case), by replaying data truncated cleanly at each such boundary through
// replayFn. refFor then answers "what must a truncation at length L look
// like" by picking the largest precomputed boundary <= L.
type referenceStates struct {
	offsets []int64
	rows    [][][]any
}

func buildReferenceStates(t *testing.T, data []byte, boundaries []recordBoundary, replayFn func(*testing.T, []byte) [][]any) *referenceStates {
	t.Helper()
	rs := &referenceStates{offsets: []int64{0}, rows: [][][]any{nil}}
	for _, b := range boundaries {
		if !b.isCommit {
			continue
		}
		rows := replayFn(t, data[:b.offset])
		rs.offsets = append(rs.offsets, b.offset)
		rs.rows = append(rs.rows, rows)
	}
	return rs
}

func (rs *referenceStates) at(l int64) [][]any {
	var best [][]any
	for i, off := range rs.offsets {
		if off <= l {
			best = rs.rows[i]
		} else {
			break
		}
	}
	return best
}

func readFixture(t *testing.T, name string) []byte {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(walFixturesDir, name))
	if err != nil {
		t.Fatalf("read fixture %s: %v", name, err)
	}
	return data
}

// TestWALManagerCrashTruncationNeverPartiallyApplies truncates
// walmanager_legacy.wal at every record boundary and at several mid-record
// byte offsets, and asserts that replaying each truncated file always
// matches replaying the fixture cleanly cut at the end of the last
// transaction that fully fits — i.e. a transaction interrupted mid-way is
// never partially applied; it is either whole or entirely absent.
func TestWALManagerCrashTruncationNeverPartiallyApplies(t *testing.T) {
	data := readFixture(t, "walmanager_legacy.wal")
	boundaries := walManagerRecordBoundaries(data)
	if len(boundaries) == 0 {
		t.Fatal("no records decoded from walmanager_legacy.wal; fixture may be stale/corrupt")
	}
	if last := boundaries[len(boundaries)-1]; last.offset != int64(len(data)) {
		t.Fatalf("boundary scan stopped at %d, but fixture is %d bytes; fixture may have trailing garbage", last.offset, len(data))
	}

	refs := buildReferenceStates(t, data, boundaries, func(t *testing.T, prefix []byte) [][]any {
		rows, status, err := replayWALManagerPrefix(t, prefix)
		if err != nil {
			t.Fatalf("reference replay of a clean commit-boundary prefix (len %d) unexpectedly errored: %v", len(prefix), err)
		}
		if status.Truncated {
			t.Fatalf("reference replay of a clean commit-boundary prefix (len %d) unexpectedly reported Truncated=true", len(prefix))
		}
		return rows
	})

	for _, l := range truncationCandidates(boundaries) {
		if l <= 0 || l >= int64(len(data)) {
			continue // 0 and the full file are degenerate; full file is covered by wal_fixture_test.go
		}
		l := l
		t.Run(fmt.Sprintf("truncate_at_%d_of_%d", l, len(data)), func(t *testing.T) {
			rows, _, err := replayWALManagerPrefix(t, data[:l])
			if err != nil {
				t.Fatalf("truncated at byte %d: OpenDB returned a hard error instead of a clean Truncated result: %v", l, err)
			}
			want := refs.at(l)
			if !reflect.DeepEqual(rows, want) {
				t.Fatalf("truncated at byte %d: recovered rows %#v, want %#v (the last fully-committed transaction's state) — a transaction was partially applied", l, rows, want)
			}
		})
	}
}

// TestAdvancedWALCrashTruncationNeverPartiallyApplies is
// TestWALManagerCrashTruncationNeverPartiallyApplies' counterpart for
// advancedwal_legacy.wal / ModeAdvancedWAL recovery.
func TestAdvancedWALCrashTruncationNeverPartiallyApplies(t *testing.T) {
	data := readFixture(t, "advancedwal_legacy.wal")
	boundaries := advancedWALRecordBoundaries(data)
	if len(boundaries) == 0 {
		t.Fatal("no records decoded from advancedwal_legacy.wal; fixture may be stale/corrupt")
	}
	if last := boundaries[len(boundaries)-1]; last.offset != int64(len(data)) {
		t.Fatalf("boundary scan stopped at %d, but fixture is %d bytes; fixture may have trailing garbage", last.offset, len(data))
	}

	refs := buildReferenceStates(t, data, boundaries, replayAdvancedWALPrefix)

	for _, l := range truncationCandidates(boundaries) {
		if l <= 0 || l >= int64(len(data)) {
			continue
		}
		l := l
		t.Run(fmt.Sprintf("truncate_at_%d_of_%d", l, len(data)), func(t *testing.T) {
			rows := replayAdvancedWALPrefix(t, data[:l])
			want := refs.at(l)
			if !reflect.DeepEqual(rows, want) {
				t.Fatalf("truncated at byte %d: recovered rows %#v, want %#v (the last fully-committed transaction's state) — a transaction was partially applied", l, rows, want)
			}
		})
	}
}

// ─────────────────────────── bit-flip corruption ─────────────────────────────

// flipByte returns a copy of data with bit 0 of the byte at offset flipped.
func flipByte(data []byte, offset int64) []byte {
	out := make([]byte, len(data))
	copy(out, data)
	out[offset] ^= 0x01
	return out
}

// decodeAdvancedWALRecordAt decodes exactly the (0-indexed) idx-th WALRecord
// from data from a fresh Decoder, discarding the ones before it.
func decodeAdvancedWALRecordAt(data []byte, idx int) (WALRecord, error) {
	dec := gob.NewDecoder(bytes.NewReader(data))
	var rec WALRecord
	for i := 0; i <= idx; i++ {
		rec = WALRecord{}
		if err := dec.Decode(&rec); err != nil {
			return WALRecord{}, err
		}
	}
	return rec, nil
}

// decodeWALManagerRecordAt is decodeAdvancedWALRecordAt's counterpart for
// walRecord (WALManager's format).
func decodeWALManagerRecordAt(data []byte, idx int) (walRecord, error) {
	dec := gob.NewDecoder(bytes.NewReader(data))
	var rec walRecord
	for i := 0; i <= idx; i++ {
		rec = walRecord{}
		if err := dec.Decode(&rec); err != nil {
			return walRecord{}, err
		}
	}
	return rec, nil
}

// TestAdvancedWALBitFlipCorruptionIsDetected flips one payload byte inside
// several different records of advancedwal_legacy.wal and asserts the
// corruption is actually caught: AdvancedWAL's real CRC32-Castagnoli
// checksum (see wal_advanced.go's calculateChecksum, which covers every
// field including the before/after row images) must not match, so Recover
// stops at that record — meaning the corrupted record, and everything
// logged after it, must NOT be applied. That is exactly the same
// "reference state at the last clean commit boundary" this file already
// computes for truncation, so a bit-flip must produce identical recovered
// rows to truncating the file at the start of the corrupted record's
// enclosing transaction.
//
// Not every candidate byte offset is a meaningful corruption: gob prefixes
// the first record of a given type in the stream with a full type
// descriptor (a comparatively large, redundant metadata blob describing
// field names/types), and a single flipped bit landing inside that
// descriptor — rather than inside the record's own field values — can
// legitimately decode to the exact same Go value it would have without the
// flip. Asserting detection for a flip that never actually changed
// anything would be testing gob's wire format, not AdvancedWAL's checksum,
// so each candidate is first decoded on both the clean and corrupted bytes
// and skipped (not asserted) if the decoded record is unchanged.
//
// A second, narrower carve-out surfaced empirically while building this
// test: calculateChecksum hashes record.Timestamp via UnixNano(), which is
// — by design — invariant to a time.Time's embedded monotonic reading and
// (perhaps less obviously) to how gob happens to reconstruct its location
// metadata; a bit flip landing in exactly those bytes can decode to a
// time.Time that *prints* differently (different wall-clock minute/offset
// in %v) yet has the identical UnixNano(), so the checksum — correctly, by
// its own definition — sees no change. That is a real, narrow blind spot
// in what AdvancedWAL's checksum covers (Timestamp's displayed value can
// drift without detection, though this has no bearing on any row data or
// on recovery correctness, since Timestamp is never used to decide what to
// apply), worth a mention for the consolidation work, but distinct from
// — and much narrower than — WALManager's total absence of a checksum. It
// is logged, not asserted, for the same reason as a true no-op: nothing
// this test cares about (row images, Tenant/Table/RowID, OpType, TxID)
// actually changed.
func TestAdvancedWALBitFlipCorruptionIsDetected(t *testing.T) {
	data := readFixture(t, "advancedwal_legacy.wal")
	boundaries := advancedWALRecordBoundaries(data)
	refs := buildReferenceStates(t, data, boundaries, replayAdvancedWALPrefix)

	// sansTimestamp zeroes the one field this test doesn't treat as a
	// meaningful corruption on its own (see the doc comment above).
	sansTimestamp := func(r WALRecord) WALRecord {
		r.Timestamp = time.Time{}
		return r
	}

	prev := int64(0)
	flips, noops, timestampOnly := 0, 0, 0
	for i, b := range boundaries {
		// Flip a handful of representative offsets inside this record's
		// payload (skip the first couple of bytes, which are more likely to
		// be gob's shared type descriptor rather than this record's own
		// value bytes).
		start := prev + 2
		for _, frac := range []float64{0.25, 0.5, 0.75} {
			off := start + int64(float64(b.offset-start)*frac)
			if off <= prev || off >= b.offset {
				continue
			}
			boundaryOffset := b.offset
			t.Run(fmt.Sprintf("record_%d_flip_at_%d", i, off), func(t *testing.T) {
				corrupted := flipByte(data, off)
				cleanRec, err := decodeAdvancedWALRecordAt(data, i)
				if err != nil {
					t.Fatalf("decode clean record %d: %v", i, err)
				}
				corruptRec, decErr := decodeAdvancedWALRecordAt(corrupted, i)
				if decErr == nil && reflect.DeepEqual(cleanRec, corruptRec) {
					noops++
					t.Skipf("flip at byte %d decoded to the identical record (landed in shared gob type-descriptor bytes, not this record's own data) — nothing was actually corrupted, so there is nothing to detect", off)
				}
				if decErr == nil && reflect.DeepEqual(sansTimestamp(cleanRec), sansTimestamp(corruptRec)) {
					timestampOnly++
					t.Skipf("flip at byte %d only changed how Timestamp prints, not its UnixNano() value (clean=%v corrupt=%v) — calculateChecksum hashes UnixNano(), so this is a real but narrow checksum blind spot around Timestamp's location metadata, not a row-data corruption; see the doc comment above", off, cleanRec.Timestamp, corruptRec.Timestamp)
				}
				flips++

				rows := replayAdvancedWALPrefix(t, corrupted)
				// The corrupted record's transaction (and anything after it
				// in the file) must not be applied: recovered rows must
				// match the state as of just before this record's
				// transaction, i.e. exactly what a truncation right before
				// it would also produce.
				wantIfCaught := refs.at(prev)
				fullFileWant := refs.at(int64(len(data)))
				refBefore := refs.at(prev)
				if reflect.DeepEqual(rows, fullFileWant) && !reflect.DeepEqual(refBefore, fullFileWant) {
					t.Fatalf("flip at byte %d (inside record ending at %d, decode error=%v): checksum did NOT catch the corruption — recovered the full, uncorrupted final state despite a flipped payload byte that demonstrably changed the decoded record", off, boundaryOffset, decErr)
				}
				if !reflect.DeepEqual(rows, wantIfCaught) {
					t.Fatalf("flip at byte %d: recovered rows %#v, want %#v (state just before the corrupted record's transaction) — checksum detection must stop recovery cleanly, never misapply a corrupted record", off, rows, wantIfCaught)
				}
			})
		}
		prev = b.offset
	}
	if flips == 0 {
		t.Fatal("no genuinely-corrupting bit-flip offsets were exercised; fixture may be too small")
	}
	t.Logf("exercised %d genuine bit-flips (all detected), %d were no-ops (shared gob type descriptor), %d changed only Timestamp's displayed value (UnixNano()-invariant)", flips, noops, timestampOnly)
}

// TestWALManagerBitFlipCorruptionIsNotDetected documents a real,
// currently-shipping gap rather than a bug to fix under this stage:
// WALManager's on-disk walRecord (db.go) carries no checksum field at all,
// and replayWAL never verifies one (confirmed by reading db.go — see
// writeRecord/replayWAL). A single flipped payload byte inside an
// otherwise-structurally-valid gob record is therefore *not* guaranteed to
// be caught. Depending on exactly which byte gets flipped, one of two
// things happens, and this test accepts either without failing:
//
//   - gob's own framing incidentally breaks (a flipped length-prefix or
//     type-descriptor byte), which surfaces as a decode error and is
//     handled exactly like a truncation — safe, if accidental.
//   - the flipped byte lands inside a fixed-width value (this test
//     specifically targets FLOAT64Type's 8-byte payload, i.e. an account
//     balance) and gob decodes it just fine as a different, silently wrong
//     number. Nothing here can detect that a value changed underneath it.
//
// This is exactly the gap the later WAL-consolidation stages are meant to
// close by giving ModeWAL a real checksum for the first time (see
// AdvancedWAL's CRC32-Castagnoli in wal_advanced.go for what that will look
// like). It is intentionally not fixed here — db.go is frozen for this
// stage — and this test exists so the gap has a concrete, reproducible
// regression harness once that work begins, instead of being an unverified
// claim in a comment.
func TestWALManagerBitFlipCorruptionIsNotDetected(t *testing.T) {
	data := readFixture(t, "walmanager_legacy.wal")
	boundaries := walManagerRecordBoundaries(data)
	refs := buildReferenceStates(t, data, boundaries, func(t *testing.T, prefix []byte) [][]any {
		rows, _, err := replayWALManagerPrefix(t, prefix)
		if err != nil {
			t.Fatalf("reference replay of a clean commit-boundary prefix (len %d) unexpectedly errored: %v", len(prefix), err)
		}
		return rows
	})

	prev := int64(0)
	detected, silent, noops := 0, 0, 0
	for i, b := range boundaries {
		start := prev + 2
		for _, frac := range []float64{0.25, 0.5, 0.75} {
			off := start + int64(float64(b.offset-start)*frac)
			if off <= prev || off >= b.offset {
				continue
			}
			corrupted := flipByte(data, off)

			// Skip byte flips that land in shared gob type-descriptor bytes
			// and therefore decode to the exact same record content — see
			// TestAdvancedWALBitFlipCorruptionIsDetected's doc comment for
			// why this happens and why it isn't itself informative.
			if cleanRec, err := decodeWALManagerRecordAt(data, i); err == nil {
				if corruptRec, err := decodeWALManagerRecordAt(corrupted, i); err == nil && reflect.DeepEqual(cleanRec, corruptRec) {
					noops++
					t.Logf("record %d flip at byte %d: no-op (landed in shared gob type-descriptor bytes)", i, off)
					continue
				}
			}

			rows, status, replayErr := replayWALManagerPrefix(t, corrupted)
			refBeforeTx := refs.at(prev)
			fullFileWant := refs.at(int64(len(data)))
			switch {
			case replayErr != nil, status.Truncated, reflect.DeepEqual(rows, refBeforeTx) && !reflect.DeepEqual(refBeforeTx, fullFileWant):
				// gob framing broke incidentally (either OpenDB itself
				// errored, or replayWAL truncated cleanly): treated exactly
				// like a truncation right before this record's transaction.
				// Safe, if accidental — WALManager still has no actual
				// checksum.
				detected++
				t.Logf("record %d flip at byte %d: incidentally caught (decode error=%v / Truncated=%v)", i, off, replayErr, status.Truncated)
			default:
				// Decoded "successfully" and produced a value that doesn't
				// match the pre-transaction state: a silently corrupted
				// value made it into the live table. This is the documented
				// gap — see the doc comment above.
				silent++
				t.Logf("record %d flip at byte %d: SILENTLY MISAPPLIED (no checksum to catch it) — recovered %#v", i, off, rows)
			}
			// Regardless of outcome, corruption must never leave a
			// structurally broken row (e.g. wrong cell count) in the live
			// table — silent value corruption is the documented gap, but a
			// malformed row would be a more severe, unbounded failure mode.
			for _, row := range rows {
				if len(row) != 3 {
					t.Fatalf("flip at byte %d produced a structurally malformed row (want 3 cells): %#v", off, row)
				}
			}
		}
		prev = b.offset
	}
	if silent == 0 {
		t.Fatal("expected at least one bit-flip to be silently misapplied (that is the documented gap this test exists to demonstrate); every flip was either a no-op or incidentally caught — if WALManager now has a checksum, update this test's doc comment and expectations")
	}
	t.Logf("WALManager bit-flip summary: %d incidentally caught, %d silently misapplied (checksum gap), %d no-ops", detected, silent, noops)
}
