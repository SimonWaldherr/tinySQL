package engine

import (
	"context"
	"fmt"
	"math/rand"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// A differential test of the secondary indexes under DELETE (and everything that
// surrounds it). Every statement runs on two databases that hold the same table:
// "idx" has four secondary indexes (a composite over two TEXT columns with NULL
// keys, a UNIQUE one, a single INT column, a composite INT + TEXT), "ref" has none
// and therefore answers from table scans. After each statement
//
//   - the rows of both must be identical, in the same order,
//   - every index of idx must equal an index built from its rows (the same keys,
//     the same row positions: Len and one lookup per key, plus prefix lookups),
//   - queries that use the indexes must return what scans of ref return.
//
// Statements run on the live databases and inside transactions that use the table
// sharing, index borrowing and twin machinery of the real commit path, with
// savepoints, rollbacks, commits, snapshot/restore round trips and (in the disk
// variant) close and reopen. Statements that fail half way must leave nothing
// behind.

const rmTable = `CREATE TABLE r (id INT, uid INT, grp TEXT, sub TEXT, n INT, note TEXT, v FLOAT)`

var rmIndexes = []string{
	`CREATE INDEX ix_grp_sub ON r(grp, sub)`,
	`CREATE UNIQUE INDEX ix_uid ON r(uid)`,
	`CREATE INDEX ix_n ON r(n)`,
	`CREATE INDEX ix_n_grp ON r(n, grp)`,
}

type rmPair struct{ idx, ref *storage.DB }

// rmSavepoint is a savepoint's copy of the transaction and the number of rows
// removing DELETEs the transaction had run when it was taken.
type rmSavepoint struct {
	idx, ref *storage.DB
	deletes  int
}

type rmStats struct {
	statements, deletes, rowsDeleted, failed, commits, rollbacks, savepointRollbacks int
	reopens, roundTrips, emptyDeletes, allRows, returning, indexedDeletes            int
	// returningRolledBack counts DELETEs whose RETURNING projection failed after
	// the rows and the indexes had been changed (the statement must undo both).
	// dirtySavepointRollbacks counts ROLLBACK TO that undid at least one removing
	// DELETE. txDeletes counts removing DELETEs that ran inside a transaction.
	returningRolledBack, dirtySavepointRollbacks, txDeletes int
	// indexTwinTakes counts the skip lists that transactions took from a twin
	// instead of copying (storage.TwinTakes) over the whole run.
	indexTwinTakes int
}

type rmWorld struct {
	t       *testing.T
	r       *rand.Rand
	live    rmPair
	inTx    bool
	base    rmPair // the transaction's conflict-detection bases
	shadow  rmPair
	release [2]func()
	saves   []rmSavepoint
	disk    bool
	dir     string
	files   int
	path    string
	nextID  int
	nextUID int
	offset  int
	stats   *rmStats
	// txDeletes counts the removing DELETEs of the open transaction that are still
	// in effect (a rollback to a savepoint takes back the ones after it).
	txDeletes int
	// twinHeavy runs many short transactions over a big table (see run).
	twinHeavy bool
}

func rmRun(db *storage.DB, sql string) (*ResultSet, error) {
	return Execute(context.Background(), db, "default", mustParse(sql))
}

func rmMust(t *testing.T, db *storage.DB, sql string) *ResultSet {
	t.Helper()
	rs, err := rmRun(db, sql)
	if err != nil {
		t.Fatalf("%.200s: %v", sql, err)
	}
	return rs
}

func newRMWorld(t *testing.T, seed int64, disk bool, stats *rmStats) *rmWorld {
	t.Helper()
	w := &rmWorld{t: t, r: rand.New(rand.NewSource(seed)), disk: disk, stats: stats, nextID: 1, nextUID: 1, dir: t.TempDir()}
	if disk {
		w.path = w.dir + "/db"
		db, err := storage.OpenDB(storage.StorageConfig{Mode: storage.ModeDisk, Path: w.path})
		if err != nil {
			t.Fatal(err)
		}
		w.live.idx = db
	} else {
		w.live.idx = storage.NewDB()
	}
	w.live.ref = storage.NewDB()
	t.Cleanup(func() {
		if w.inTx {
			w.endTx(false)
		}
		_ = w.live.idx.Close()
		_ = w.live.ref.Close()
	})
	rmMust(t, w.live.idx, rmTable)
	rmMust(t, w.live.ref, rmTable)
	for _, stmt := range rmIndexes {
		rmMust(t, w.live.idx, stmt)
	}
	rows := 80 + w.r.Intn(700)
	if seed%3 == 0 {
		// Above the twin threshold (storage.rowTwinMinRows = 256) for the whole run,
		// so commits park index twins and later transactions take them.
		rows = 1500 + w.r.Intn(600)
		w.twinHeavy = true
	}
	for rows > 0 {
		k := min(rows, 1+w.r.Intn(40))
		sql := w.insertSQL(k)
		rmMust(t, w.live.idx, sql)
		rmMust(t, w.live.ref, sql)
		rows -= k
	}
	return w
}

func (w *rmWorld) cur() rmPair {
	if w.inTx {
		return w.shadow
	}
	return w.live
}

func (w *rmWorld) table(db *storage.DB) *storage.Table {
	w.t.Helper()
	table, err := db.Get("default", "r")
	if err != nil {
		w.t.Fatal(err)
	}
	return table
}

// ---------------------------------------------------------------------------
// Statements

func (w *rmWorld) lit(s string, nullShare float64) string {
	if w.r.Float64() < nullShare {
		return "NULL"
	}
	return "'" + s + "'"
}

func (w *rmWorld) insertSQL(k int) string {
	var sb strings.Builder
	sb.WriteString(`INSERT INTO r VALUES `)
	for i := 0; i < k; i++ {
		if i > 0 {
			sb.WriteString(", ")
		}
		n := "NULL"
		if w.r.Float64() > 0.1 {
			n = fmt.Sprint(w.r.Intn(12) - 3)
		}
		sub := fmt.Sprintf("s%d", w.r.Intn(5))
		if w.r.Intn(20) == 0 {
			sub = ""
		}
		fmt.Fprintf(&sb, "(%d, %d, %s, %s, %s, %s, %v)", w.nextID, w.nextUID,
			w.lit(fmt.Sprintf("g%d", w.r.Intn(6)), 0.1), w.lit(sub, 0.1), n,
			w.lit(fmt.Sprintf("note %d", w.r.Intn(30)), 0.1), float64(w.r.Intn(1000))/10)
		w.nextID++
		w.nextUID++
	}
	return sb.String()
}

type rmOp struct {
	sql       string
	deletes   bool
	returning bool
	// failingReturning marks a DELETE whose RETURNING list divides by zero for every
	// row with a non-NULL n; where is its predicate.
	failingReturning bool
	where            string
}

func (w *rmWorld) someID() int {
	ref := w.table(w.cur().ref)
	if len(ref.Rows) == 0 {
		return 0
	}
	return ref.Rows[w.r.Intn(len(ref.Rows))][0].(int)
}

func (w *rmWorld) idAt(first bool) int {
	ref := w.table(w.cur().ref)
	if len(ref.Rows) == 0 {
		return 0
	}
	if first {
		return ref.Rows[0][0].(int)
	}
	return ref.Rows[len(ref.Rows)-1][0].(int)
}

func (w *rmWorld) someUID() int {
	ref := w.table(w.cur().ref)
	if len(ref.Rows) == 0 {
		return 1
	}
	return ref.Rows[w.r.Intn(len(ref.Rows))][1].(int)
}

func (w *rmWorld) grp() string { return fmt.Sprintf("g%d", w.r.Intn(6)) }
func (w *rmWorld) sub() string { return fmt.Sprintf("s%d", w.r.Intn(5)) }

// narrowDeleteOp removes a few rows, so that a table that is kept big by inserts
// stays above the twin threshold while DELETEs run between transactions.
func (w *rmWorld) narrowDeleteOp() rmOp {
	var where string
	switch w.r.Intn(6) {
	case 0:
		where = fmt.Sprintf(`id = %d`, w.someID())
	case 1:
		where = fmt.Sprintf(`grp = '%s' AND sub = '%s' AND n = %d`, w.grp(), w.sub(), w.r.Intn(8)-2)
	case 2:
		where = fmt.Sprintf(`uid = %d`, w.someUID())
	case 3:
		a := w.someID()
		where = fmt.Sprintf(`id >= %d AND id < %d`, a, a+1+w.r.Intn(6))
	case 4:
		where = fmt.Sprintf(`grp = '%s' AND sub = '%s' AND note = 'note %d'`, w.grp(), w.sub(), w.r.Intn(30))
	default:
		where = fmt.Sprintf(`grp = '%s' AND sub <> '%s' AND n = %d`, w.grp(), w.sub(), w.r.Intn(8)-2)
	}
	op := rmOp{deletes: true, where: where, sql: `DELETE FROM r WHERE ` + where}
	switch w.r.Intn(10) {
	case 0:
		op.returning, op.failingReturning = true, true
		op.sql += ` RETURNING id, 10 / (n - n)`
	case 1:
		op.returning = true
		op.sql += ` RETURNING id, uid, grp`
	}
	return op
}

// randomTwinOp is the statement mix of the twin-heavy scenarios: mostly writes that
// the index journal records (INSERT, UPDATE), with DELETEs in between that make
// it give up.
func (w *rmWorld) randomTwinOp() rmOp {
	switch k := w.r.Intn(100); {
	case k < 40:
		return rmOp{sql: w.insertSQL(1 + w.r.Intn(40))}
	case k < 65:
		return w.updateOp()
	default:
		return w.narrowDeleteOp()
	}
}

func (w *rmWorld) randomOp() rmOp {
	if w.twinHeavy {
		return w.randomTwinOp()
	}
	switch k := w.r.Intn(100); {
	case k < 12:
		return rmOp{sql: w.insertSQL(1 + w.r.Intn(6))}
	case k < 14:
		// A multi-row INSERT whose last row repeats an existing UID: it fails after
		// the first rows were appended and must leave nothing behind.
		sql := w.insertSQL(1 + w.r.Intn(4))
		return rmOp{sql: sql + fmt.Sprintf(", (%d, %d, 'g0', 's0', 1, 'dup', 1.5)", w.nextID, w.someUID())}
	case k < 34:
		return w.updateOp()
	default:
		return w.deleteOp()
	}
}

func (w *rmWorld) updateOp() rmOp {
	switch w.r.Intn(8) {
	case 0:
		return rmOp{sql: fmt.Sprintf(`UPDATE r SET n = n + 1 WHERE grp = '%s'`, w.grp())}
	case 1:
		return rmOp{sql: fmt.Sprintf(`UPDATE r SET note = 'u%d' WHERE id %% 3 = %d`, w.r.Intn(9), w.r.Intn(3))}
	case 2:
		return rmOp{sql: fmt.Sprintf(`UPDATE r SET grp = '%s', sub = '%s' WHERE n >= %d AND n <= %d`, w.grp(), w.sub(), w.r.Intn(5), 3+w.r.Intn(6))}
	case 3:
		w.offset += 1_000_000
		return rmOp{sql: fmt.Sprintf(`UPDATE r SET uid = uid + %d WHERE id < %d`, w.offset, w.someID())}
	case 4:
		// Fails as soon as two rows get the same UID; succeeds for one row.
		return rmOp{sql: fmt.Sprintf(`UPDATE r SET uid = %d WHERE grp = '%s'`, w.someUID(), w.grp())}
	case 5:
		return rmOp{sql: fmt.Sprintf(`UPDATE r SET n = NULL WHERE sub = '%s'`, w.sub())}
	case 6:
		return rmOp{sql: fmt.Sprintf(`UPDATE r SET sub = NULL WHERE id = %d`, w.someID())}
	default:
		return rmOp{sql: fmt.Sprintf(`UPDATE r SET v = v + 1 WHERE n = %d`, w.r.Intn(8)-2)}
	}
}

func (w *rmWorld) deleteOp() rmOp {
	var where string
	switch w.r.Intn(20) {
	case 0:
		where = fmt.Sprintf(`grp = '%s'`, w.grp())
	case 1:
		where = fmt.Sprintf(`grp = '%s' AND sub <> '%s'`, w.grp(), w.sub())
	case 2:
		a := w.someID()
		where = fmt.Sprintf(`id >= %d AND id < %d`, a, a+1+w.r.Intn(12))
	case 3:
		where = fmt.Sprintf(`id = %d`, w.someID())
	case 4:
		where = fmt.Sprintf(`id = %d`, w.idAt(true))
	case 5:
		where = fmt.Sprintf(`id = %d`, w.idAt(false))
	case 6:
		where = fmt.Sprintf(`n IN (%d, %d, %d)`, w.r.Intn(10)-2, w.r.Intn(10)-2, w.r.Intn(10)-2)
	case 7:
		where = fmt.Sprintf(`grp IN ('%s', '%s')`, w.grp(), w.grp())
	case 8:
		where = `n IS NULL`
	case 9:
		where = fmt.Sprintf(`id < %d`, w.idAt(true)) // matches nothing
	case 10:
		where = fmt.Sprintf(`id IN (SELECT id FROM r WHERE n > %d)`, 5+w.r.Intn(4))
	case 11:
		where = fmt.Sprintf(`n > %d OR grp = '%s'`, 6+w.r.Intn(4), w.grp())
	case 12:
		where = fmt.Sprintf(`(id * 2) %% 5 = %d`, w.r.Intn(5))
	case 13:
		where = fmt.Sprintf(`n = %d AND grp = '%s'`, w.r.Intn(8)-2, w.grp())
	case 14:
		where = fmt.Sprintf(`uid = %d`, w.someUID())
	case 15:
		where = fmt.Sprintf(`grp = '%s' AND sub = '%s'`, w.grp(), w.sub())
	case 16:
		// Fails on the first row with n = 3, after earlier rows were selected.
		where = `10 / (n - 3) > 1`
	case 17:
		where = fmt.Sprintf(`note = 'note %d'`, w.r.Intn(30))
	case 18:
		if w.r.Intn(6) == 0 {
			where = `1 = 1`
		} else {
			where = fmt.Sprintf(`n >= %d AND n <= %d`, w.r.Intn(4), 4+w.r.Intn(6))
		}
	default:
		where = fmt.Sprintf(`sub = '%s'`, w.sub())
	}
	op := rmOp{deletes: true, where: where}
	if w.r.Intn(8) == 0 {
		// The projection fails after the rows left the table and the indexes were
		// renumbered; the statement has to take both back.
		op.returning = true
		op.failingReturning = true
		op.sql = `DELETE FROM r WHERE ` + where + ` RETURNING id, 10 / (n - n)`
	} else if w.r.Intn(5) == 0 {
		op.returning = true
		op.sql = `DELETE FROM r WHERE ` + where + ` RETURNING id, uid, grp`
	} else {
		op.sql = `DELETE FROM r WHERE ` + where
	}
	if w.r.Intn(40) == 0 {
		op.sql = `DELETE FROM r`
		op.returning = false
		op.failingReturning = false
	}
	return op
}

// apply runs op on the idx database and, when it succeeded there, on ref, which
// must then succeed with the same answer. A statement that failed on idx left
// nothing behind, which check() verifies against the untouched ref.
func (w *rmWorld) apply(op rmOp) {
	w.t.Helper()
	cur := w.cur()
	w.stats.statements++
	if op.deletes {
		if stmt, ok := mustParse(op.sql).(*Delete); ok && stmt.Where != nil {
			table := w.table(cur.idx)
			if _, indexed := deleteCandidateRows(table, simpleColumnIndex(table, "r"), stmt.Where); indexed {
				w.stats.indexedDeletes++
			}
		}
	}
	// Rows that the failing RETURNING list cannot survive: the statement is expected
	// to fail, and what it did to the table before failing must be undone.
	doomed := 0
	if op.failingReturning {
		if rs, err := rmRun(cur.ref, `SELECT COUNT(*) AS c FROM r WHERE (`+op.where+`) AND n IS NOT NULL`); err == nil && len(rs.Rows) == 1 {
			doomed, _ = rs.Rows[0]["c"].(int)
		}
	}
	got, err := rmRun(cur.idx, op.sql)
	if err != nil {
		w.stats.failed++
		if doomed > 0 {
			w.stats.returningRolledBack++
		}
		return
	}
	if doomed > 0 {
		w.t.Fatalf("%.200s: succeeded although %d rows have a failing RETURNING value", op.sql, doomed)
	}
	want, err := rmRun(cur.ref, op.sql)
	if err != nil {
		w.t.Fatalf("ref failed where idx succeeded: %.200s: %v", op.sql, err)
	}
	if !reflect.DeepEqual(rmRows(got), rmRows(want)) {
		w.t.Fatalf("%.200s: idx answered %v, ref %v", op.sql, rmRows(got), rmRows(want))
	}
	if op.deletes {
		w.stats.deletes++
		deleted := 0
		if op.returning {
			deleted = len(rmRows(got))
			w.stats.returning++
		} else if len(rmRows(got)) == 1 {
			deleted, _ = rmRows(got)[0]["deleted"].(int)
		}
		w.stats.rowsDeleted += deleted
		if deleted == 0 {
			w.stats.emptyDeletes++
		} else if w.inTx {
			w.txDeletes++
			w.stats.txDeletes++
		}
		if len(w.table(cur.ref).Rows) == 0 {
			w.stats.allRows++
		}
	}
}

// ---------------------------------------------------------------------------
// Checks

// rmCheckIndexes compares every index of table with one built from its rows.
func rmCheckIndexes(t *testing.T, table *storage.Table, label string) {
	t.Helper()
	for name, idx := range table.Indexes {
		pos := make([]int, len(idx.Columns))
		for i, column := range idx.Columns {
			p, err := table.ColIndex(column)
			if err != nil {
				t.Fatal(err)
			}
			pos[i] = p
		}
		type group struct {
			values []any
			rows   []int
		}
		groups := map[string]*group{}
		prefixes := map[string][]int{}
		for rowID, row := range table.Rows {
			values := make([]any, len(pos))
			for i, p := range pos {
				values[i] = row[p]
			}
			key := string(storage.CanonicalIndexKey(values))
			g := groups[key]
			if g == nil {
				g = &group{values: values}
				groups[key] = g
			}
			g.rows = append(g.rows, rowID)
			first := string(storage.CanonicalIndexKey(values[:1]))
			prefixes[first] = append(prefixes[first], rowID)
		}
		if idx.Len() != len(groups) {
			t.Fatalf("%s: index %s holds %d keys, its rows have %d", label, name, idx.Len(), len(groups))
		}
		for _, g := range groups {
			got, err := table.LookupSecondaryIndexPoint(idx, g.values)
			if err != nil || !slices.Equal(got, g.rows) {
				t.Fatalf("%s: index %s key %v holds rows %v, want %v (%v)", label, name, g.values, got, g.rows, err)
			}
			if len(idx.Columns) > 1 {
				first := string(storage.CanonicalIndexKey(g.values[:1]))
				got, err := table.LookupSecondaryIndexPrefix(idx, g.values[:1])
				if err != nil || !slices.Equal(got, prefixes[first]) {
					t.Fatalf("%s: index %s prefix %v holds rows %v, want %v (%v)", label, name, g.values[:1], got, prefixes[first], err)
				}
			}
		}
	}
}

// queries returns SELECTs that the indexes answer, with parameters drawn from the
// current rows.
func (w *rmWorld) queries(k int) []string {
	cols := `id, uid, grp, sub, n, note`
	var out []string
	for i := 0; i < k; i++ {
		switch w.r.Intn(10) {
		case 0:
			out = append(out, fmt.Sprintf(`SELECT %s FROM r WHERE grp = '%s'`, cols, w.grp()))
		case 1:
			out = append(out, fmt.Sprintf(`SELECT %s FROM r WHERE grp = '%s' AND sub = '%s'`, cols, w.grp(), w.sub()))
		case 2:
			out = append(out, fmt.Sprintf(`SELECT %s FROM r WHERE n = %d`, cols, w.r.Intn(10)-2))
		case 3:
			out = append(out, fmt.Sprintf(`SELECT %s FROM r WHERE n >= %d AND n < %d`, cols, w.r.Intn(4)-2, 2+w.r.Intn(8)))
		case 4:
			out = append(out, fmt.Sprintf(`SELECT %s FROM r WHERE uid = %d`, cols, w.someUID()))
		case 5:
			out = append(out, fmt.Sprintf(`SELECT %s FROM r WHERE n = %d AND grp = '%s'`, cols, w.r.Intn(10)-2, w.grp()))
		case 6:
			out = append(out, fmt.Sprintf(`SELECT %s FROM r WHERE grp = '%s' AND sub >= '%s'`, cols, w.grp(), w.sub()))
		case 7:
			out = append(out, fmt.Sprintf(`SELECT %s FROM r WHERE grp IN ('%s', '%s')`, cols, w.grp(), w.grp()))
		case 8:
			out = append(out, `SELECT COUNT(*), SUM(n) FROM r`)
		default:
			out = append(out, fmt.Sprintf(`SELECT %s FROM r WHERE grp = '%s' AND n > %d`, cols, w.grp(), w.r.Intn(6)))
		}
	}
	return out
}

func (w *rmWorld) compareQueries(label string, idx, ref *storage.DB, k int) {
	w.t.Helper()
	for _, q := range w.queries(k) {
		got, err := rmRun(idx, q)
		if err != nil {
			w.t.Fatalf("%s: %s: %v", label, q, err)
		}
		want, err := rmRun(ref, q)
		if err != nil {
			w.t.Fatalf("%s: %s (ref): %v", label, q, err)
		}
		if !reflect.DeepEqual(rmRows(got), rmRows(want)) {
			w.t.Fatalf("%s: %s\n idx: %v\n ref: %v", label, q, got.Rows, want.Rows)
		}
	}
}

func (w *rmWorld) checkPair(label string, p rmPair, queries int) {
	w.t.Helper()
	idx, ref := w.table(p.idx), w.table(p.ref)
	if len(idx.Rows) != len(ref.Rows) {
		w.t.Fatalf("%s: idx has %d rows, ref %d", label, len(idx.Rows), len(ref.Rows))
	}
	for i := range ref.Rows {
		if !reflect.DeepEqual(idx.Rows[i], ref.Rows[i]) {
			w.t.Fatalf("%s: row %d is %v on idx, %v on ref", label, i, idx.Rows[i], ref.Rows[i])
		}
	}
	rmCheckIndexes(w.t, idx, label)
	w.compareQueries(label, p.idx, p.ref, queries)
}

// check verifies the live databases and, inside a transaction, the transaction's
// copy as well: a write inside a transaction must never reach the live tables.
func (w *rmWorld) check(label string) {
	w.t.Helper()
	w.checkPair(label+" (live)", w.live, 3)
	if w.inTx {
		w.checkPair(label+" (transaction)", w.shadow, 3)
	}
}

// ---------------------------------------------------------------------------
// Transactions, savepoints, persistence

func (w *rmWorld) beginTx() {
	w.t.Helper()
	w.inTx = true
	for i, db := range []*storage.DB{w.live.idx, w.live.ref} {
		db.LockContentForRead()
		base, shadow, release := db.SnapshotForTxShared()
		db.UnlockContentForRead()
		if i == 0 {
			w.base.idx, w.shadow.idx = base, shadow
		} else {
			w.base.ref, w.shadow.ref = base, shadow
		}
		w.release[i] = release
	}
	w.saves = nil
}

func (w *rmWorld) endTx(commit bool) {
	w.t.Helper()
	if commit {
		for i, db := range []*storage.DB{w.live.idx, w.live.ref} {
			base, shadow := w.base.idx, w.shadow.idx
			if i == 1 {
				base, shadow = w.base.ref, w.shadow.ref
			}
			db.LockContentForWrite()
			err := db.ApplyWALChanges(storage.CollectWALChanges(base, shadow))
			db.UnlockContentForWrite()
			if err != nil {
				w.t.Fatal(err)
			}
		}
		w.stats.commits++
	} else {
		w.stats.rollbacks++
	}
	w.release[0]()
	w.release[1]()
	w.inTx = false
	w.saves = nil
	w.txDeletes = 0
}

func (w *rmWorld) savepoint() {
	sp := rmSavepoint{deletes: w.txDeletes}
	for i, db := range []*storage.DB{w.live.idx, w.live.ref} {
		shadow := w.shadow.idx
		if i == 1 {
			shadow = w.shadow.ref
		}
		db.LockContentForRead()
		saved := shadow.SnapshotForWriteTx()
		db.UnlockContentForRead()
		if i == 0 {
			sp.idx = saved
		} else {
			sp.ref = saved
		}
	}
	w.saves = append(w.saves, sp)
}

func (w *rmWorld) rollbackToSavepoint() {
	at := w.r.Intn(len(w.saves))
	sp := w.saves[at]
	// The savepoint copy is never written; each rollback takes a fresh writable
	// shadow, so the savepoint stays usable (see conn.rollbackToSavepoint).
	w.shadow.idx = sp.idx.SnapshotForWriteTx()
	w.shadow.ref = sp.ref.SnapshotForWriteTx()
	w.saves = w.saves[:at+1]
	w.stats.savepointRollbacks++
	if w.txDeletes > sp.deletes {
		w.stats.dirtySavepointRollbacks++
	}
	w.txDeletes = sp.deletes
}

// persist round-trips the idx database through its persistent form. The memory
// variant goes through a gob checkpoint file, the disk variant through close and
// reopen; the reopened database then continues as the live one, with indexes that
// exist only as persisted entries until the first statement touches them.
func (w *rmWorld) persist() {
	w.t.Helper()
	if w.disk {
		if err := w.live.idx.Close(); err != nil {
			w.t.Fatal(err)
		}
		db, err := storage.OpenDB(storage.StorageConfig{Mode: storage.ModeDisk, Path: w.path})
		if err != nil {
			w.t.Fatal(err)
		}
		w.live.idx = db
		w.stats.reopens++
		return
	}
	w.files++
	file := fmt.Sprintf("%s/r%d.gob", w.dir, w.files)
	if err := storage.SaveToFile(w.live.idx, file); err != nil {
		w.t.Fatal(err)
	}
	loaded, err := storage.LoadFromFile(file)
	if err != nil {
		w.t.Fatal(err)
	}
	w.stats.roundTrips++
	w.checkPair("after a checkpoint round trip", rmPair{idx: loaded, ref: w.live.ref}, 4)
	if w.r.Intn(2) == 0 {
		_ = w.live.idx.Close()
		w.live.idx = loaded
	} else {
		_ = loaded.Close()
	}
}

func (w *rmWorld) run(ops int) {
	w.t.Helper()
	w.check("initial state")
	for i := 0; i < ops; i++ {
		label := fmt.Sprintf("after statement %d", i)
		k := w.r.Intn(100)
		if w.twinHeavy {
			// Short transactions one after the other, so that a commit parks twins
			// and the next transaction takes them; the savepoint and persistence
			// cases keep their share.
			switch {
			case !w.inTx && k < 45:
				k = 0
			case !w.inTx && k < 48:
				k = 20
			case w.inTx && k < 30:
				k = 7
			case w.inTx && k < 38:
				k = 10
			case w.inTx && k < 44:
				k = 15
			default:
				k = 99
			}
		}
		switch {
		case k < 6 && !w.inTx:
			w.beginTx()
			label = "after BEGIN"
		case k < 9 && w.inTx:
			w.endTx(w.r.Intn(2) == 0)
			label = "after the transaction ended"
		case k < 13 && w.inTx:
			w.savepoint()
			label = "after SAVEPOINT"
		case k < 19 && w.inTx && len(w.saves) > 0:
			w.rollbackToSavepoint()
			label = "after ROLLBACK TO SAVEPOINT"
		case k < 22 && !w.inTx:
			w.persist()
			label = "after persisting"
		default:
			w.apply(w.randomOp())
		}
		w.check(label)
	}
	if w.inTx {
		w.endTx(w.r.Intn(2) == 0)
		w.check("after the last transaction")
	}
}

func rmRunScenarios(t *testing.T, disk bool, seeds int) *rmStats {
	t.Helper()
	was := storage.SetTwinVerification(true)
	t.Cleanup(func() { storage.SetTwinVerification(was) })
	stats := &rmStats{}
	_, twinsBefore := storage.TwinTakes()
	for seed := 1; seed <= seeds; seed++ {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			newRMWorld(t, int64(seed), disk, stats).run(60)
		})
	}
	_, twinsAfter := storage.TwinTakes()
	stats.indexTwinTakes = int(twinsAfter - twinsBefore)
	return stats
}

func TestDeleteKeepsSecondaryIndexesConsistent(t *testing.T) {
	seeds := 40
	if testing.Short() {
		seeds = 8
	}
	stats := rmRunScenarios(t, false, seeds)
	t.Logf("%+v", *stats)
	if testing.Short() {
		return
	}
	// The scenarios must actually have exercised what they claim to.
	if stats.deletes < 200 || stats.rowsDeleted < 500 || stats.emptyDeletes == 0 || stats.allRows == 0 ||
		stats.failed < 20 || stats.commits == 0 || stats.rollbacks == 0 ||
		stats.savepointRollbacks == 0 || stats.roundTrips == 0 || stats.returning == 0 || stats.indexedDeletes < 100 ||
		stats.returningRolledBack < 5 || stats.dirtySavepointRollbacks < 5 || stats.txDeletes < 20 || stats.indexTwinTakes == 0 {
		t.Fatalf("the scenarios covered too little: %+v", *stats)
	}
}

func TestDeleteKeepsSecondaryIndexesConsistentOnDisk(t *testing.T) {
	seeds := 16
	if testing.Short() {
		seeds = 4
	}
	stats := rmRunScenarios(t, true, seeds)
	t.Logf("%+v", *stats)
	if !testing.Short() && (stats.reopens == 0 || stats.deletes < 80) {
		t.Fatalf("the scenarios covered too little: %+v", *stats)
	}
}

func rmRows(rs *ResultSet) []Row {
	if rs == nil {
		return nil
	}
	return rs.Rows
}
