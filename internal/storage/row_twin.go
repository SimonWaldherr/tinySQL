package storage

import (
	"os"
	"reflect"
	"sync/atomic"
	"unsafe"
)

// Row twins: a second row-header buffer that lets a transaction take its
// private copy of a table without copying the table.
//
// A transaction's first write to a table gives it a private *Table whose Rows
// is a fresh copy of the live table's row headers (cloneTableForSharedTx). That
// copy is 24 bytes per row, and it was the last part of a one-row transaction
// that grew with the table: 2.4 MB for 100000 rows, nearly all of the
// transaction's cost once the garbage collector had scanned it.
//
// At COMMIT the private table becomes the live one and the table it was copied
// from is dropped. The dropped table's row-header array differs from the new
// live one only where the transaction changed rows. Instead of leaving it to
// the garbage collector, ApplyWALChanges brings it up to date (catchUpRows:
// replay the rows the transaction updated, append the rows it inserted) and
// parks it on the new live table as its twin. The next transaction that writes
// the table takes the twin as its private Rows, so its first write costs the
// rows the previous transactions changed, not the rows the table holds. The two
// arrays then swap roles at every commit.
//
// What makes this safe:
//
//   - Only an unpinned table is recycled. A table some stream or transaction
//     still reads keeps its array; the copy-on-write protocol in
//     stream_snapshot.go and tx_share.go already requires every reader that
//     outlives the content lock to hold a pin, because an unpinned live table is
//     updated in place by autocommit statements. Recycling writes the array
//     under the same content write lock, at the moment the table stops being
//     live.
//   - What changed is taken from the table's structural log (structVersion,
//     rowUpdateLog), the record that already tells an incremental vector index
//     which rows moved: UPDATE lists its rows, INSERT only appends, everything
//     else (DELETE, DDL, cascades, replay) invalidates the log, and then no twin
//     is kept.
//   - A row array that was replaced wholesale rather than written slot by slot
//     (a rebuilt table, a statement rollback, an append that outgrew its
//     capacity) is recognised by its address and never paired with a twin.
//   - A twin is only kept for tables whose columns are all scalar. Rows of other
//     tables hold cells a transaction must not share (see cloneRowsShared).
//
// Every doubt resolves to "no twin", which costs one ordinary copy. The price
// of a twin is memory: one extra row-header array for each table that
// transactions write to.

// rowTwinMinRows is the table size below which copying the row headers is
// cheaper than keeping a second array around.
const rowTwinMinRows = 256

// rowTwin is a spare row-header array attached to the table it mirrors.
type rowTwin struct {
	// rows equalled the owner's Rows when the owner's structVersion was
	// structVersion and its Rows began at backing with len(rows) rows.
	rows          [][]any
	structVersion int
	backing       *[]any
}

// rowTwinOrigin records, on a transaction's private copy of a table, what it
// was copied from. ApplyWALChanges uses it to decide whether the table the copy
// replaces can become the copy's twin.
type rowTwinOrigin struct {
	source        *Table
	version       int    // source.Version at copy time
	structVersion int    // source.structVersion at copy time
	rowCount      int    // len(source.Rows) at copy time
	sourceBacking *[]any // where source.Rows began at copy time
	backing       *[]any // where the copy's Rows began at copy time
}

// verifyRowTwins makes every twin hand-over compare the whole array against the
// table it mirrors and panic on a difference. It is a test aid: the comparison
// costs what the twin exists to avoid.
var verifyRowTwins = os.Getenv("TINYSQL_VERIFY_ROW_TWINS") != ""

// SetTwinVerification switches the full comparison on or off and returns the
// previous setting. Tests use it; it must not be flipped while statements run.
func SetTwinVerification(on bool) (previous bool) {
	previous = verifyRowTwins
	verifyRowTwins = on
	return previous
}

// twinRowTakes and twinIndexTakes count the copies twins have saved.
var twinRowTakes, twinIndexTakes atomic.Uint64

// TwinTakes reports how many row-header arrays and secondary-index skip lists
// were handed to a transaction from a twin instead of being copied.
func TwinTakes() (rows, indexes uint64) {
	return twinRowTakes.Load(), twinIndexTakes.Load()
}

// catchUpRows brings buf, which held t's rows when t.structVersion was since
// and t had len(buf) rows, up to t's current rows. It returns false when the
// structural log cannot account for every difference; buf is then unusable.
func (t *Table) catchUpRows(buf [][]any, since int) ([][]any, bool) {
	n := len(buf)
	if len(t.Rows) < n || since > t.structVersion {
		return nil, false
	}
	if since != t.structVersion {
		log := &t.rowUpdateLog
		total := log.len()
		if since < t.rowUpdateBase || total == 0 {
			return nil, false
		}
		want := since + 1
		for i := log.search(want); i < total; i++ {
			delta := log.at(i)
			if delta.structVersion != want || delta.row < 0 || delta.row >= len(t.Rows) {
				return nil, false
			}
			want++
			if delta.row < n {
				buf[delta.row] = t.Rows[delta.row]
			}
		}
		if want != t.structVersion+1 {
			return nil, false
		}
	}
	return append(buf, t.Rows[n:]...), true
}

// takeRowTwin returns a row-header array equal to t.Rows that the caller owns,
// or nil when t has no usable twin. t must not change during the call: the
// caller holds a pin on it or the content write lock.
func (t *Table) takeRowTwin() [][]any {
	twin := t.twin.Swap(nil)
	if twin == nil || twin.backing != unsafe.SliceData(t.Rows) || !t.onlyScalarColumns() {
		return nil
	}
	rows, ok := t.catchUpRows(twin.rows, twin.structVersion)
	if !ok {
		return nil
	}
	if verifyRowTwins {
		mustMirror(rows, t.Rows, "take")
	}
	twinRowTakes.Add(1)
	return rows
}

// adoptTwins runs when next replaces old as the live table at COMMIT. If next
// is the transaction's private copy of exactly this old table and nothing still
// reads old, old's row-header array and its secondary-index skip lists are
// brought up to date and kept as next's twins. The caller holds db's content
// write lock.
func (db *DB) adoptTwins(old, next *Table) {
	origin := next.twinOrigin
	next.twinOrigin = nil // do not keep the replaced table reachable
	if origin == nil || old == nil || old == next || origin.source != old ||
		// The source must be exactly what it was when the copy was taken.
		origin.structVersion != old.structVersion ||
		origin.rowCount != len(old.Rows) ||
		db.IsTablePinnedForStream(old) {
		forgetIndexOrigins(next)
		return
	}
	adoptRowTwin(origin, old, next)
	adoptIndexTwins(old, next)
}

func adoptRowTwin(origin *rowTwinOrigin, old, next *Table) {
	if len(next.Rows) < rowTwinMinRows || !next.onlyScalarColumns() {
		return
	}
	// Both tables must still live in the arrays they had when the copy was
	// taken.
	if origin.backing != unsafe.SliceData(next.Rows) ||
		origin.sourceBacking != unsafe.SliceData(old.Rows) ||
		origin.backing == origin.sourceBacking {
		return
	}
	rows, ok := next.catchUpRows(old.Rows, origin.structVersion)
	if !ok {
		return
	}
	if verifyRowTwins {
		mustMirror(rows, next.Rows, "adopt")
	}
	old.twin.Store(nil)
	next.twin.Store(&rowTwin{rows: rows, structVersion: next.structVersion, backing: unsafe.SliceData(next.Rows)})
}

// mustMirror panics unless got and want hold the same rows.
func mustMirror(got, want [][]any, stage string) {
	if len(got) != len(want) {
		panic("tinysql: row twin (" + stage + ") has a different row count than its table")
	}
	for i := range want {
		if len(got[i]) == len(want[i]) && (len(want[i]) == 0 || &got[i][0] == &want[i][0]) {
			continue
		}
		if !reflect.DeepEqual(got[i], want[i]) {
			panic("tinysql: row twin (" + stage + ") differs from its table")
		}
	}
}

// returnTwins runs when a transaction ends without having committed t, its
// private copy of a table (rollback, a conflict, or a transaction whose
// statements changed nothing). If the copy is still exactly what it was copied
// from, its row-header array and index skip lists go back to the source as
// twins, so a transaction that wrote nothing does not cost the next one a full
// copy. shadow is the transaction's database.
func (shadow *DB) returnTwins(t *Table) {
	origin := t.twinOrigin
	t.twinOrigin = nil
	if origin == nil || shadow.IsTablePinnedForStream(t) ||
		t.Version != origin.version || t.structVersion != origin.structVersion ||
		len(t.Rows) != origin.rowCount || unsafe.SliceData(t.Rows) != origin.backing {
		return
	}
	if origin.rowCount >= rowTwinMinRows && origin.backing != origin.sourceBacking && t.onlyScalarColumns() {
		origin.source.twin.CompareAndSwap(nil, &rowTwin{
			rows:          t.Rows,
			structVersion: origin.structVersion,
			backing:       origin.sourceBacking,
		})
	}
	for _, idx := range t.Indexes {
		if idx != nil {
			idx.returnTwin()
		}
	}
}
