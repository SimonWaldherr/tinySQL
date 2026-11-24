package engine

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// tryExec runs one statement and returns its error instead of failing the
// test, so goroutines can report through t.Errorf.
func tryExec(db *storage.DB, sql string) (*ResultSet, error) {
	p := NewParser(sql)
	stmt, err := p.ParseStatement()
	if err != nil {
		return nil, err
	}
	return Execute(context.Background(), db, "default", stmt)
}

// copyTree copies a storage directory (flat files below one tenant directory).
func copyTree(t *testing.T, src, dst string) {
	t.Helper()
	err := filepath.Walk(src, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(src, path)
		target := filepath.Join(dst, rel)
		if info.IsDir() {
			return os.MkdirAll(target, 0o755)
		}
		if strings.HasSuffix(path, ".tmp") {
			return nil
		}
		in, err := os.Open(path)
		if err != nil {
			return err
		}
		defer in.Close()
		out, err := os.Create(target)
		if err != nil {
			return err
		}
		defer out.Close()
		_, err = io.Copy(out, in)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
}

// checkIndexes verifies that every persisted secondary index describes exactly
// the persisted rows: each entry's key is the key of the indexed columns of the
// row it points at, and every row is indexed once.
func checkIndexes(t *testing.T, tb *storage.Table, name string) bool {
	t.Helper()
	for iname, idx := range tb.Indexes {
		pos := make([]int, len(idx.Columns))
		for i, c := range idx.Columns {
			p, err := tb.ColIndex(c)
			if err != nil {
				t.Errorf("%s index %s: column %s: %v", name, iname, c, err)
				return false
			}
			pos[i] = p
		}
		count := 0
		for _, e := range idx.Entries {
			count += len(e.RowIDs)
			for _, rid := range e.RowIDs {
				if rid < 0 || rid >= len(tb.Rows) {
					t.Errorf("%s index %s points at row %d of %d", name, iname, rid, len(tb.Rows))
					return false
				}
				vals := make([]any, len(pos))
				for i, p := range pos {
					vals[i] = tb.Rows[rid][p]
				}
				if !bytes.Equal(e.Key, storage.CanonicalIndexKey(vals)) {
					t.Errorf("%s index %s: the entry for row %d has the wrong key (row values %v)", name, iname, rid, vals)
					return false
				}
			}
		}
		if count != len(tb.Rows) {
			t.Errorf("%s index %s holds %d row ids for %d rows", name, iname, count, len(tb.Rows))
			return false
		}
	}
	return true
}

func sortedIDs(tb *storage.Table) []int {
	ids := make([]int, 0, len(tb.Rows))
	for _, row := range tb.Rows {
		ids = append(ids, row[0].(int))
	}
	sort.Ints(ids)
	return ids
}

// checkPersistedSnapshot loads the files of dir through a fresh backend and
// verifies what a torn snapshot would break: money is only ever moved between
// accounts by one statement, so the balances must always add up; the tables px
// and py always get the same id in one critical section, so the persisted tables
// (written from one snapshot critical section) must hold the same ids; and every
// index must match its rows.
func checkPersistedSnapshot(t *testing.T, dir string, accounts, total int) {
	t.Helper()
	b, err := storage.NewDiskBackend(dir, false)
	if err != nil {
		t.Errorf("open copy: %v", err)
		return
	}
	load := func(name string) *storage.Table {
		tb, err := b.LoadTable("default", name)
		if err != nil || tb == nil {
			t.Errorf("load %s: %v", name, err)
			return nil
		}
		return tb
	}
	acct, events, px, py := load("acct"), load("events"), load("px"), load("py")
	if acct == nil || events == nil || px == nil || py == nil {
		return
	}
	if len(acct.Rows) != accounts {
		t.Errorf("persisted acct has %d rows, want %d", len(acct.Rows), accounts)
		return
	}
	sum := 0
	for _, row := range acct.Rows {
		sum += row[1].(int)
	}
	if sum != total {
		t.Errorf("persisted balances add up to %d, want %d: a torn snapshot", sum, total)
	}
	seen := make(map[int]bool, len(events.Rows))
	for _, row := range events.Rows {
		id := row[0].(int)
		if seen[id] {
			t.Errorf("persisted events hold id %d twice", id)
			return
		}
		seen[id] = true
	}
	if !reflect.DeepEqual(sortedIDs(px), sortedIDs(py)) {
		t.Errorf("persisted px holds %d ids and py %d: the two tables come from different instants", len(px.Rows), len(py.Rows))
	}
	for name, tb := range map[string]*storage.Table{"acct": acct, "events": events, "px": px, "py": py} {
		if !checkIndexes(t, tb, name) {
			return
		}
	}
}

func queryRows(t *testing.T, db *storage.DB, sql string) []Row {
	t.Helper()
	return execSQL(t, db, sql).Rows
}

// Writers, readers and repeated Syncs run against the same database; every
// persisted snapshot is consistent and the final state survives a reopen.
func TestSyncUnderConcurrentWritersPersistsConsistentSnapshots(t *testing.T) {
	duration := 2 * time.Second
	if testing.Short() {
		duration = 600 * time.Millisecond
	}
	for _, mode := range []storage.StorageMode{storage.ModeDisk, storage.ModeHybrid} {
		t.Run(mode.String(), func(t *testing.T) {
			dir := t.TempDir()
			cfg := storage.StorageConfig{Mode: mode, Path: dir, MaxMemoryBytes: 256 << 20}
			db, err := storage.OpenDB(cfg)
			if err != nil {
				t.Fatal(err)
			}
			const accounts, start = 20, 100
			execSQL(t, db, `CREATE TABLE acct (id INT, bal INT)`)
			execSQL(t, db, `CREATE INDEX idx_bal ON acct (bal)`)
			for i := 0; i < accounts; i++ {
				execSQL(t, db, fmt.Sprintf(`INSERT INTO acct VALUES (%d, %d)`, i, start))
			}
			execSQL(t, db, `CREATE TABLE events (id INT, kind INT, body TEXT)`)
			execSQL(t, db, `CREATE INDEX idx_kind ON events (kind)`)
			execSQL(t, db, `CREATE TABLE px (id INT)`)
			execSQL(t, db, `CREATE TABLE py (id INT)`)
			execSQL(t, db, `CREATE TABLE par (pid INT PRIMARY KEY)`)
			execSQL(t, db, `INSERT INTO par VALUES (1)`)
			execSQL(t, db, `CREATE TABLE chi (id INT, ref INT FOREIGN KEY REFERENCES par(pid))`)
			execSQL(t, db, `INSERT INTO chi VALUES (1, 1)`)
			px, _ := db.Get("default", "px")
			py, _ := db.Get("default", "py")

			stop := make(chan struct{})
			var wg sync.WaitGroup
			var syncs, snapshotsChecked atomic.Int64
			baseSeed := time.Now().UnixNano()
			copyRoot := t.TempDir()
			var workers atomic.Int64
			t.Logf("seed %d", baseSeed)
			run := func(f func(r *rand.Rand)) {
				wg.Add(1)
				go func(seed int64) {
					defer wg.Done()
					r := rand.New(rand.NewSource(seed))
					for {
						select {
						case <-stop:
							return
						default:
							f(r)
						}
					}
				}(baseSeed + workers.Add(1))
			}
			fail := func(sql string, err error) {
				t.Errorf("%s: %v", sql, err)
			}

			for w := 0; w < 2; w++ {
				run(func(r *rand.Rand) {
					a, b := r.Intn(accounts), r.Intn(accounts)
					if a == b {
						return
					}
					sql := fmt.Sprintf(`UPDATE acct SET bal = CASE WHEN id = %d THEN bal - 1 WHEN id = %d THEN bal + 1 ELSE bal END WHERE id IN (%d, %d)`, a, b, a, b)
					if _, err := tryExec(db, sql); err != nil {
						fail(sql, err)
					}
				})
			}
			var nextEvent atomic.Int64
			run(func(r *rand.Rand) {
				n := nextEvent.Add(1)
				sql := fmt.Sprintf(`INSERT INTO events VALUES (%d, %d, 'event %d')`, n, n%5, n)
				if _, err := tryExec(db, sql); err != nil {
					fail(sql, err)
				}
			})
			run(func(r *rand.Rand) {
				for _, sql := range []string{
					fmt.Sprintf(`DELETE FROM events WHERE kind = %d`, r.Intn(5)),
					`UPDATE events SET kind = kind + 1 WHERE kind < 4`,
				} {
					if _, err := tryExec(db, sql); err != nil {
						fail(sql, err)
					}
					time.Sleep(time.Millisecond)
				}
			})
			// Two tables always get the same id inside one critical section of the
			// content lock: a persisted pair from different instants is a torn Sync.
			var nextPair atomic.Int64
			run(func(r *rand.Rand) {
				n := int(nextPair.Add(1))
				db.LockContentForWrite()
				px.Rows = append(px.Rows, []any{n})
				px.Version++
				py.Rows = append(py.Rows, []any{n})
				py.Version++
				db.UnlockContentForWrite()
				time.Sleep(200 * time.Microsecond)
			})
			// DDL during Syncs: RENAME COLUMN rewrites the foreign key of the child
			// table in place; indexes are created and dropped; a column is added.
			var ddlRounds atomic.Int64
			run(func(r *rand.Rand) {
				n := ddlRounds.Add(1)
				from, to := "pid", "pid2"
				if n%2 == 0 {
					from, to = "pid2", "pid"
				}
				for _, sql := range []string{
					fmt.Sprintf(`ALTER TABLE par RENAME COLUMN %s TO %s`, from, to),
					`CREATE INDEX idx_tmp ON events (id)`,
					`DROP INDEX idx_tmp ON events`,
					fmt.Sprintf(`INSERT INTO chi VALUES (%d, 1)`, n+1),
				} {
					if _, err := tryExec(db, sql); err != nil {
						fail(sql, err)
					}
				}
				time.Sleep(time.Millisecond)
			})
			for rd := 0; rd < 2; rd++ {
				run(func(r *rand.Rand) {
					rs, err := tryExec(db, `SELECT SUM(bal) AS s FROM acct`)
					if err != nil {
						fail("SELECT SUM", err)
						return
					}
					if got := rs.Rows[0]["s"]; got != accounts*start {
						t.Errorf("live balances add up to %v, want %d", got, accounts*start)
					}
					if _, err := tryExec(db, `SELECT COUNT(*) AS c FROM events WHERE kind = 2`); err != nil {
						fail("SELECT COUNT", err)
					}
				})
			}
			run(func(r *rand.Rand) {
				if err := db.Sync(); err != nil {
					t.Errorf("Sync: %v", err)
					return
				}
				syncs.Add(1)
				// Nothing else syncs, so the files are stable until this loop
				// comes round again: copy and verify them.
				copyDir := filepath.Join(copyRoot, "copy")
				if err := os.RemoveAll(copyDir); err != nil {
					t.Errorf("clear copy: %v", err)
					return
				}
				copyTree(t, dir, copyDir)
				checkPersistedSnapshot(t, copyDir, accounts, accounts*start)
				snapshotsChecked.Add(1)
			})

			time.Sleep(duration)
			close(stop)
			wg.Wait()
			if t.Failed() {
				return
			}
			if syncs.Load() < 2 {
				t.Fatalf("only %d Syncs completed", syncs.Load())
			}

			// The in-memory truth, then the same after Sync, Close and reopen.
			wantAcct := queryRows(t, db, `SELECT id, bal FROM acct ORDER BY id`)
			wantEvents := queryRows(t, db, `SELECT id, kind, body FROM events ORDER BY id`)
			if err := db.Sync(); err != nil {
				t.Fatal(err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			reopened, err := storage.OpenDB(cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			if got := queryRows(t, reopened, `SELECT id, bal FROM acct ORDER BY id`); !reflect.DeepEqual(got, wantAcct) {
				t.Fatalf("acct differs after reopen")
			}
			if got := queryRows(t, reopened, `SELECT id, kind, body FROM events ORDER BY id`); !reflect.DeepEqual(got, wantEvents) {
				t.Fatalf("events differ after reopen: %d rows, want %d", len(got), len(wantEvents))
			}
			for kind := 0; kind < 6; kind++ {
				q := fmt.Sprintf(`SELECT COUNT(*) AS c FROM events WHERE kind = %d`, kind)
				want := 0
				for _, row := range wantEvents {
					if row["kind"] == kind {
						want++
					}
				}
				if got := queryRows(t, reopened, q); len(got) != 1 || got[0]["c"] != want {
					t.Fatalf("%s after reopen = %v, want %d (the index and the rows disagree)", q, got, want)
				}
			}
			pxR, _ := reopened.Get("default", "px")
			pyR, _ := reopened.Get("default", "py")
			if !reflect.DeepEqual(sortedIDs(pxR), sortedIDs(pyR)) || len(pxR.Rows) != int(nextPair.Load()) {
				t.Fatalf("px/py after reopen hold %d/%d rows, %d pairs were inserted", len(pxR.Rows), len(pyR.Rows), nextPair.Load())
			}
			parR, _ := reopened.Get("default", "par")
			chiR, _ := reopened.Get("default", "chi")
			if got, want := chiR.Cols[1].ForeignKey.Column, parR.Cols[0].Name; got != want {
				t.Fatalf("child foreign key references %q after reopen, the parent column is %q", got, want)
			}
			t.Logf("%s: %d syncs, %d persisted snapshots verified, %d events inserted, %d rows left", mode, syncs.Load(), snapshotsChecked.Load(), nextEvent.Load(), len(wantEvents))
		})
	}
}

// bigVectorWords, bigVectorVocab and bigVectorZipf shape the text of the rows
// bigVectorTable builds (the measurement test overrides them).
var (
	bigVectorWords = 25
	bigVectorVocab = 5000
	bigVectorZipf  = false
)

// bigVectorTable builds a table of rows x dims float64 vectors directly (the
// SQL path would spend minutes on the inserts).
func bigVectorTable(name string, rows, dims int) *storage.Table {
	tbl := storage.NewTable(name, []storage.Column{
		{Name: "id", Type: storage.IntType},
		{Name: "body", Type: storage.TextType},
		{Name: "emb", Type: storage.VectorType},
	}, false)
	tbl.Rows = make([][]any, rows)
	r := rand.New(rand.NewSource(7))
	var zipf *rand.Zipf
	if bigVectorZipf {
		zipf = rand.NewZipf(r, 1.1, 1, uint64(bigVectorVocab-1))
	}
	for i := range tbl.Rows {
		v := make([]float64, dims)
		for d := range v {
			v[d] = r.Float64()
		}
		var body strings.Builder
		fmt.Fprintf(&body, "chunk %d", i)
		for w := 0; w < bigVectorWords; w++ {
			word := r.Intn(bigVectorVocab)
			if zipf != nil {
				word = int(zipf.Uint64())
			}
			fmt.Fprintf(&body, " word%d", word)
		}
		tbl.Rows[i] = []any{i, body.String(), v}
	}
	tbl.Version = 1
	return tbl
}

func vectorLiteral(dims int, seed float64) string {
	var sb strings.Builder
	sb.WriteByte('[')
	for d := 0; d < dims; d++ {
		if d > 0 {
			sb.WriteByte(',')
		}
		fmt.Fprintf(&sb, "%g", seed+float64(d)/1000)
	}
	sb.WriteByte(']')
	return sb.String()
}

type latencies struct {
	n   int
	max time.Duration
	sum time.Duration
}

func (l *latencies) add(d time.Duration) {
	l.n++
	l.sum += d
	if d > l.max {
		l.max = d
	}
}

// While one Sync writes a table that takes seconds to encode, statements on
// the same database (reads, and DML on the very table being written) stay in
// the millisecond range.
func TestSyncDoesNotStallStatementsWhileWriting(t *testing.T) {
	rows, dims := 200_000, 128
	if testing.Short() {
		rows = 60_000
	}
	dir := t.TempDir()
	db, err := storage.OpenDB(storage.StorageConfig{Mode: storage.ModeDisk, Path: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Put("default", bigVectorTable("big", rows, dims)); err != nil {
		t.Fatal(err)
	}
	execSQL(t, db, `CREATE TABLE small (id INT, v INT)`)
	execSQL(t, db, `INSERT INTO small VALUES (5, 0)`)
	// Make the big table dirty so that Sync has to write all of it.
	execSQL(t, db, fmt.Sprintf(`INSERT INTO big (id, body, emb) VALUES (%d, 'late', '%s')`, rows, vectorLiteral(dims, 0.5)))
	if err := db.Sync(); err != nil { // warm-up: first full write, caches
		t.Fatal(err)
	}
	execSQL(t, db, fmt.Sprintf(`INSERT INTO big (id, body, emb) VALUES (%d, 'later', '%s')`, rows+1, vectorLiteral(dims, 0.25)))

	var syncDur time.Duration
	done := make(chan struct{})
	started := make(chan struct{})
	go func() {
		close(started)
		s := time.Now()
		if err := db.Sync(); err != nil {
			t.Errorf("Sync: %v", err)
		}
		syncDur = time.Since(s)
		close(done)
	}()
	<-started

	var sel, upd, ins, scan latencies
	next := rows + 2
	measure := func(l *latencies, sql string) {
		s := time.Now()
		if _, err := tryExec(db, sql); err != nil {
			t.Errorf("%s: %v", sql, err)
		}
		l.add(time.Since(s))
	}
loop:
	for {
		select {
		case <-done:
			break loop
		default:
		}
		measure(&sel, `SELECT v FROM small WHERE id = 5`)
		measure(&upd, `UPDATE small SET v = v + 1 WHERE id = 5`)
		measure(&ins, fmt.Sprintf(`INSERT INTO big (id, body, emb) VALUES (%d, 'during sync', '%s')`, next, vectorLiteral(dims, float64(next%7))))
		next++
		measure(&scan, `SELECT COUNT(*) AS c FROM big WHERE id = 3`)
		time.Sleep(5 * time.Millisecond)
	}
	worst := max(sel.max, upd.max, ins.max, scan.max)
	t.Logf("Sync %v for %d rows x %d dims; statements during it: SELECT n=%d max=%v, UPDATE n=%d max=%v, INSERT(big) n=%d max=%v, scan n=%d max=%v; snapshot lock hold %v",
		syncDur, rows, dims, sel.n, sel.max, upd.n, upd.max, ins.n, ins.max, scan.n, scan.max, db.LastSyncSnapshotHold())
	if sel.n < 5 {
		t.Fatalf("only %d statements completed during a %v Sync: they were blocked", sel.n, syncDur)
	}
	if worst > 750*time.Millisecond && worst > syncDur/2 { // a stall lasts about the whole Sync; the deterministic proof is storage.TestNoLockHeldWhileTheSnapshotIsEncoded
		t.Fatalf("a statement took %v during a %v Sync", worst, syncDur)
	}
	if hold := db.LastSyncSnapshotHold(); hold > 500*time.Millisecond {
		t.Fatalf("Sync held the content lock for %v", hold)
	}

	// Everything written during the Sync is durable after the next one.
	if err := db.Sync(); err != nil {
		t.Fatal(err)
	}
	want := queryRows(t, db, `SELECT COUNT(*) AS c FROM big`)
	if got := want[0]["c"]; got != rows+2+ins.n {
		t.Fatalf("big holds %v rows, want %d: statements during the Sync were lost", got, rows+2+ins.n)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := storage.OpenDB(storage.StorageConfig{Mode: storage.ModeDisk, Path: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if got := queryRows(t, reopened, `SELECT COUNT(*) AS c FROM big`); !reflect.DeepEqual(got, want) {
		t.Fatalf("row count after reopen = %v, want %v", got, want)
	}
}

// With the FTS image persisted (the default) the Sync deep-copies it under the
// table's derived-state lock, which statements that use the table's constraint
// index also take: an INSERT into a table with a primary key waits for that copy.
// The wait is bounded by the copy time (LastSyncImageCopy), is outside the
// content lock (statements on other tables and plain scans of this one are not
// held up), and is gone with SkipSearchIndexPersistence.
func TestSyncLatencyWithSearchImagesAndPrimaryKey(t *testing.T) {
	rows, dims := 60_000, 32
	if testing.Short() {
		rows = 20_000
	}
	for _, skip := range []bool{false, true} {
		t.Run(fmt.Sprintf("skip=%v", skip), func(t *testing.T) {
			dir := t.TempDir()
			db, err := storage.OpenDB(storage.StorageConfig{Mode: storage.ModeHybrid, Path: dir, MaxMemoryBytes: 1 << 30, SkipSearchIndexPersistence: skip})
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			big := bigVectorTable("big", rows, dims)
			big.Cols[0].Constraint = storage.PrimaryKey
			if err := db.Put("default", big); err != nil {
				t.Fatal(err)
			}
			execSQL(t, db, `CREATE TABLE small (id INT, v INT)`)
			execSQL(t, db, `INSERT INTO small VALUES (5, 0)`)
			execSQL(t, db, `SELECT * FROM FTS_WARM('big', 'body')`)
			ins := func(id int) string {
				return fmt.Sprintf(`INSERT INTO big (id, body, emb) VALUES (%d, 'late', '%s')`, id, vectorLiteral(dims, 0.5))
			}
			execSQL(t, db, ins(rows)) // builds the constraint index
			if err := db.Sync(); err != nil {
				t.Fatal(err)
			}
			execSQL(t, db, ins(rows+1))

			done := make(chan struct{})
			started := make(chan struct{})
			var syncDur time.Duration
			go func() {
				close(started)
				s := time.Now()
				if err := db.Sync(); err != nil {
					t.Errorf("Sync: %v", err)
				}
				syncDur = time.Since(s)
				close(done)
			}()
			<-started
			var pkIns, smallSel, scan latencies
			measure := func(l *latencies, sql string) {
				s := time.Now()
				if _, err := tryExec(db, sql); err != nil {
					t.Errorf("%s: %v", sql, err)
				}
				l.add(time.Since(s))
			}
			next := rows + 2
		loop:
			for {
				select {
				case <-done:
					break loop
				default:
				}
				measure(&pkIns, ins(next))
				next++
				measure(&smallSel, `SELECT v FROM small WHERE id = 5`)
				measure(&scan, `SELECT COUNT(*) AS c FROM big WHERE body = 'nothing'`)
				time.Sleep(2 * time.Millisecond)
			}
			imageCopy := db.LastSyncImageCopy()
			t.Logf("skip=%v: Sync %v (image copy %v, content-lock hold %v) for %d rows x %d dims; during it: PK INSERT n=%d max=%v, small SELECT n=%d max=%v, scan n=%d max=%v",
				skip, syncDur, imageCopy, db.LastSyncSnapshotHold(), rows, dims, pkIns.n, pkIns.max, smallSel.n, smallSel.max, scan.n, scan.max)
			if skip {
				if imageCopy != 0 {
					t.Fatalf("images were copied (%v) although they are skipped", imageCopy)
				}
				if worst := max(pkIns.max, smallSel.max); worst > time.Second {
					t.Fatalf("a statement took %v during a Sync with skipped images", worst)
				}
			} else {
				if imageCopy <= 0 {
					t.Fatal("no image copy was measured")
				}
				// The wait of a statement that needs the derived-state lock is
				// bounded by the copy; statements that do not need it are unaffected.
				if pkIns.max > imageCopy+time.Second {
					t.Fatalf("PK INSERT waited %v for a %v image copy", pkIns.max, imageCopy)
				}
				if smallSel.max > time.Second {
					t.Fatalf("a SELECT on another table took %v", smallSel.max)
				}
			}
		})
	}
}
