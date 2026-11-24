package engine

import (
	"context"
	"fmt"
	"io"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// End-to-end stress test of everything the retrieval-maintenance work packages
// changed, run together against an oracle.
//
// The packages are: the ordered row change log with removal events and the
// column-aware UPDATE deltas (the FTS index follows DELETEs and touch UPDATEs
// instead of being rebuilt), the flat vector column cache and the HNSW graph
// that follow DELETEs, the in-place renumbering of secondary indexes by DELETE
// together with the use of index candidates for text equality and IN
// predicates, and the snapshot-based DB.Sync with its opt-in
// SkipSearchIndexPersistence and RowBlockFiles. Each was developed and tested
// alone; this test runs the workload of the application that motivated them
// (a RAG chunk table that importers keep replacing while searches and Syncs run
// all the time) through all of them at once.
//
// A seeded writer replaces documents (INSERT the new rows of a source under a
// new load id, DELETE the old load), deletes batches of sources and ids,
// touches bookkeeping columns of tens to hundreds of rows, rewrites contents and
// embeddings, runs statements that fail halfway, and runs explicit transactions
// with savepoints that commit or roll back. It keeps an oracle (a plain map
// from id to row) that is updated only after a statement succeeded. Meanwhile
// two goroutines call DB.Sync in a loop (it serialises them, as it does in the
// application) and three readers run FTS_SEARCH, VEC_SEARCH and HYBRID_SEARCH
// continuously and check invariants that need no oracle: every hit is a row that
// exists with the identity it was born with, the content of a text hit contains
// the term, the distance a vector hit reports is the cosine distance of the
// vector it returns, ranks and scores are ordered and finite.
//
// The writer checks the live database against the oracle at random points
// (exact membership for full-text and exact vector searches, exact distances
// for the approximate ones), and at the end of every run the live database, the
// database after Sync + Close + reopen, a database reopened after a continued
// workload, and the copies of the storage directory taken at "crash" points
// (after a Sync, without Close) must all equal the oracle: the table, every
// secondary index against a scan, and every search against a fresh in-memory
// database built from the same rows (in the live table's physical order for a
// bit-for-bit comparison of everything including tie order, and in id order,
// where only the id to score maps must agree). HNSW is compared with the exact
// answer by recall only (and probed with vectors just written, see probeHNSW).
//
// Four things go beyond the obvious. A second, scalar table (side) takes part in
// the same statements, transactions and Syncs: a table with a vector column never
// gets the row and index twins that consecutive transactions hand to each other,
// and the in-place renumbering of an index by DELETE has to invalidate exactly
// those. While the writer runs, one of the Sync goroutines copies the storage
// directory (no Sync runs meanwhile), opens the copy and requires it to hold one
// of the states the writer went through, named by the versions of the two tables:
// a snapshot that mixed instants, an index lent to a snapshot and renumbered in
// place meanwhile, or a version recorded for the wrong rows show here and nowhere
// else, because the copies of the crash points are taken while nothing writes.
// Transactions are those of the driver (a shadow database, merged back with
// ApplyWALChanges). The cap on the retained UPDATE history is crossed twice per
// run with no search in between, once with a removal in front of the touches.
//
// Known limit: in ModeHybrid, a table that was loaded from the files lives in the
// buffer pool and is not copied by SnapshotForTxShared, so a transaction writes
// to it directly and its ROLLBACK does not roll back (this is the case on the base
// version v0.75.0 as well). Hybrid runs therefore use no explicit transactions
// after the first reopen; see the comment in run.
//
// Everything the writer does is determined by the seed, so a failure reports
// the seed and the first divergence; the interleaving of the background
// goroutines is statistical.

const (
	rsDims    = 8
	rsSources = 90
	// rsMinRecall is the mean recall@10 an HNSW graph that has followed hundreds
	// of removals must keep against the exact top 10. A freshly built graph over
	// these 8-dimensional uniform vectors scores 1.000 (the run logs the figure of
	// every database it verifies: with a table this small and the default search
	// breadth the graph reaches nearly every row); a graph with stale links and
	// moved vectors may lose a little. The bound only has to catch a graph that
	// points at the wrong rows (a position shift would drop the recall towards
	// 0.1), so it sits far below.
	rsMinRecall = 0.80
)

type rsRow struct {
	id      int
	source  string
	kind    string
	load    string
	loaded  int // -1 is NULL
	docDate int
	idx     int
	content string
	vec     []float64
	model   string
}

func (r *rsRow) values() string {
	loaded := "NULL"
	if r.loaded >= 0 {
		loaded = strconv.Itoa(r.loaded)
	}
	return fmt.Sprintf("(%d, '%s', '%s', '%s', %s, %d, %d, '%s', '%s', '%s')",
		r.id, r.source, r.kind, r.load, loaded, r.docDate, r.idx, r.content, retrievalVecLiteral(r.vec), r.model)
}

// rsOracle maps ids to rows. A row is never modified once it is in the map, a
// statement replaces the pointer, so a clone is a cheap, independent copy.
type rsOracle map[int]*rsRow

func (o rsOracle) clone() rsOracle {
	c := make(rsOracle, len(o))
	for id, r := range o {
		c[id] = r
	}
	return c
}

func (o rsOracle) sortedIDs() []int {
	ids := make([]int, 0, len(o))
	for id := range o {
		ids = append(ids, id)
	}
	sort.Ints(ids)
	return ids
}

func (o rsOracle) set(r *rsRow) { o[r.id] = r }

// withRow copies the row, applies f and stores the copy.
func (o rsOracle) update(id int, f func(r *rsRow)) {
	cp := *o[id]
	f(&cp)
	o[id] = &cp
}

type rsMeta struct {
	source string
	idx    int
}

type rsConfig struct {
	name   string
	mode   storage.StorageMode
	skip   bool
	blocks bool
}

func (c rsConfig) storage(path string) storage.StorageConfig {
	return storage.StorageConfig{
		Mode: c.mode, Path: path, MaxMemoryBytes: 256 << 20,
		SkipSearchIndexPersistence: c.skip, RowBlockFiles: c.blocks,
	}
}

type rsStats struct {
	// writer
	statements, replaces, deletes, deletedRows, emptyDeletes, touches, touchedRows int
	contentUpdates, embeddingUpdates, noops, failed                                int
	txCommits, txRollbacks, savepoints, savepointRollbacks, capPhases              int
	checks, crashCopies, reopens                                                   int
	// background
	syncs, reads, copyChecked, copyUnmatched atomic.Int64
}

// rsHistory is how many versions of the oracle are kept for the copies.
const rsHistory = 160

// rsFatal is what the checks panic with on a goroutine other than the test's.
type rsFatal string

type rsHarness struct {
	t            *testing.T
	seed         int64
	variant      int // 0: one index on (source_id, chunk_idx); 1: plus a single-column and a UNIQUE index
	cfg          rsConfig
	dir          string
	tenant       string
	rng          *rand.Rand
	db           *storage.DB
	oracle       rsOracle
	side         rsSide
	nextSide     int
	pendingProbe []float64
	nextID       int
	nextLd       int
	clock        int
	meta         sync.Map // id -> rsMeta, stored before the INSERT is issued
	stats        rsStats
	fresh        int

	// gate keeps Syncs out while the writer copies the storage directory.
	gate    sync.RWMutex
	errMu   sync.Mutex
	errs    []string
	failedF atomic.Bool

	crashes []rsCrash

	// history maps the Versions of the live tables to their oracles after the statement
	// that produced it (the last rsHistory versions), so that a copy of the files
	// taken while the writer runs can be compared with the state it claims to hold.
	histMu    sync.Mutex
	history   map[[2]int]rsHist
	histOrder [][2]int
	chkDir    string

	// noTx switches the explicit transactions off. A ModeHybrid database owns its
	// tables through the buffer pool after a reopen, and a transaction on such a
	// table writes through to the live one (see the comment at the reopen in run).
	noTx bool
}

type rsCrash struct {
	dir    string
	oracle rsOracle
	side   rsSide
	label  string
}

func (h *rsHarness) fail(format string, args ...any) {
	h.errMu.Lock()
	if len(h.errs) < 12 {
		h.errs = append(h.errs, fmt.Sprintf(format, args...))
	}
	h.errMu.Unlock()
	h.failedF.Store(true)
}

func (h *rsHarness) failures() string {
	h.errMu.Lock()
	defer h.errMu.Unlock()
	return strings.Join(h.errs, "\n  ")
}

func (h *rsHarness) tag() string {
	return fmt.Sprintf("seed %d variant %d %s", h.seed, h.variant, h.cfg.name)
}

func (h *rsHarness) fatalf(format string, args ...any) {
	h.t.Helper()
	h.t.Fatalf("%s: %s", h.tag(), fmt.Sprintf(format, args...))
}

// ---------------------------------------------------------------------------
// SQL helpers

func rsTry(db *storage.DB, tenant, sql string) (*ResultSet, error) {
	stmt, err := NewParser(sql).ParseStatement()
	if err != nil {
		return nil, fmt.Errorf("parse: %w", err)
	}
	return Execute(context.Background(), db, tenant, stmt)
}

// must runs a statement that has to succeed, on the goroutine of the test.
func (h *rsHarness) must(db *storage.DB, tenant, sql string) *ResultSet {
	h.t.Helper()
	rs, err := rsTry(db, tenant, sql)
	if err != nil {
		h.fatalf("%.300s: %v", sql, err)
	}
	return rs
}

func rsInt(v any) int {
	n, err := toInt(v)
	if err != nil {
		panic(fmt.Sprintf("rsInt(%T %v): %v", v, v, err))
	}
	return n
}

func rsFloat(v any) float64 {
	f, ok := v.(float64)
	if !ok {
		panic(fmt.Sprintf("rsFloat(%T %v)", v, v))
	}
	return f
}

func rsCosineDistance(a, b []float64) float64 {
	var dot, na, nb float64
	for i := range a {
		dot += a[i] * b[i]
		na += a[i] * a[i]
		nb += b[i] * b[i]
	}
	return 1 - dot/(math.Sqrt(na)*math.Sqrt(nb))
}

func rsHasToken(content, term string) bool {
	for _, f := range strings.Fields(content) {
		if f == term {
			return true
		}
	}
	return false
}

func rsHasPhrase(content string, terms []string) bool {
	f := strings.Fields(content)
	for i := 0; i+len(terms) <= len(f); i++ {
		if reflect.DeepEqual(f[i:i+len(terms)], terms) {
			return true
		}
	}
	return false
}

const rsCreateTable = `CREATE TABLE chunks (id INT, source_id TEXT, source_kind TEXT, load_id TEXT, loaded_at INT, doc_date INT, chunk_idx INT, content TEXT, embedding VECTOR, embed_model TEXT)`

func (h *rsHarness) createSchema() {
	h.must(h.db, h.tenant, rsCreateTable)
	h.must(h.db, h.tenant, `CREATE INDEX idx_src ON chunks (source_id, chunk_idx)`)
	if h.variant == 1 {
		h.must(h.db, h.tenant, `CREATE INDEX idx_load ON chunks (load_id)`)
		h.must(h.db, h.tenant, `CREATE UNIQUE INDEX uq_id ON chunks (id)`)
	}
	h.must(h.db, h.tenant, rsCreateSide)
	h.must(h.db, h.tenant, `CREATE INDEX idx_side_grp ON side (grp)`)
}

// ---------------------------------------------------------------------------
// random data

func (h *rsHarness) text() string {
	if h.rng.Intn(25) == 0 {
		return "" // a chunk without tokens
	}
	n := 2 + h.rng.Intn(9)
	words := make([]string, n)
	for i := range words {
		switch r := h.rng.Intn(10); {
		case r < 7:
			words[i] = fmt.Sprintf("w%d", h.rng.Intn(24))
		case r < 9:
			words[i] = fmt.Sprintf("tag%d", h.rng.Intn(6))
		default:
			words[i] = fmt.Sprintf("rare%d", h.rng.Intn(400))
		}
	}
	return strings.Join(words, " ")
}

func (h *rsHarness) vec() []float64 { return rsRandVec(h.rng) }

func rsSourceName(i int) string { return fmt.Sprintf("s%d", i) }

func rsKind(i int) string { return []string{"manual", "mail", "web", "sap"}[i%4] }

// newRows builds n rows of one source under a fresh load id and registers their
// identity for the readers.
func (h *rsHarness) newRows(src, n int) (rows []*rsRow, load string) {
	h.nextLd++
	load = fmt.Sprintf("L%d", h.nextLd)
	for i := 0; i < n; i++ {
		loaded := h.clock + h.rng.Intn(5)
		if h.rng.Intn(10) == 0 {
			loaded = -1
		}
		r := &rsRow{
			id: h.nextID, source: rsSourceName(src), kind: rsKind(src), load: load,
			loaded: loaded, docDate: h.rng.Intn(3000), idx: i, content: h.text(), vec: h.vec(),
			model: fmt.Sprintf("m%d", h.rng.Intn(2)),
		}
		h.nextID++
		h.meta.Store(r.id, rsMeta{source: r.source, idx: r.idx})
		rows = append(rows, r)
	}
	return rows, load
}

// ---------------------------------------------------------------------------
// the writer: statements against a target (the live database or a transaction)

type rsTarget struct {
	db *storage.DB
	o  rsOracle
	s  rsSide
}

func (h *rsHarness) insertRows(tg *rsTarget, rows []*rsRow) {
	h.t.Helper()
	if h.rng.Intn(10) < 7 {
		vals := make([]string, len(rows))
		for i, r := range rows {
			vals[i] = r.values()
		}
		h.must(tg.db, h.tenant, `INSERT INTO chunks VALUES `+strings.Join(vals, ", "))
		for _, r := range rows {
			tg.o.set(r)
		}
		h.record(tg)
	} else {
		for _, r := range rows {
			h.must(tg.db, h.tenant, `INSERT INTO chunks VALUES `+r.values())
			tg.o.set(r)
			h.record(tg)
		}
	}
}

func (h *rsHarness) deleteStmt(tg *rsTarget, sql string, match func(r *rsRow) bool) {
	h.t.Helper()
	rs := h.must(tg.db, h.tenant, sql)
	want := 0
	for id, r := range tg.o {
		if match(r) {
			delete(tg.o, id)
			want++
		}
	}
	if got := rsInt(rs.Rows[0]["deleted"]); got != want {
		h.fatalf("%s deleted %d rows, the oracle says %d", sql, got, want)
	}
	h.stats.deletes++
	h.stats.deletedRows += want
	if want == 0 {
		h.stats.emptyDeletes++
	}
	h.record(tg)
	if v := h.pendingProbe; v != nil && want > 0 {
		h.pendingProbe = nil
		h.probeHNSW(tg, v)
	}
}

func (h *rsHarness) updateStmt(tg *rsTarget, sql string, match func(r *rsRow) bool, set func(r *rsRow)) int {
	h.t.Helper()
	rs := h.must(tg.db, h.tenant, sql)
	n := 0
	for _, id := range tg.o.sortedIDs() {
		if match(tg.o[id]) {
			tg.o.update(id, set)
			n++
		}
	}
	if got := rsInt(rs.Rows[0]["updated"]); got != n {
		h.fatalf("%s updated %d rows, the oracle says %d", sql, got, n)
	}
	h.record(tg)
	return n
}

// opReplace is the importer's cycle: insert the new load of a source, then
// delete every older load of it.
func (h *rsHarness) opReplace(tg *rsTarget, src int) {
	rows, load := h.newRows(src, 4+h.rng.Intn(5))
	h.insertRows(tg, rows)
	h.stats.replaces++
	name := rsSourceName(src)
	h.deleteStmt(tg, fmt.Sprintf(`DELETE FROM chunks WHERE source_id = '%s' AND load_id <> '%s'`, name, load),
		func(r *rsRow) bool { return r.source == name && r.load != load })
}

func (h *rsHarness) sourceSet(n int) []int {
	return h.rng.Perm(rsSources)[:n]
}

func rsInList(names []string) string {
	q := make([]string, len(names))
	for i, n := range names {
		q[i] = "'" + n + "'"
	}
	return strings.Join(q, ", ")
}

func (h *rsHarness) opDeleteIn(tg *rsTarget) {
	if h.rng.Intn(3) == 0 {
		ids := tg.o.sortedIDs()
		n := 1 + h.rng.Intn(30)
		pick := map[int]bool{}
		var list []string
		for i := 0; i < n && len(ids) > 0; i++ {
			id := ids[h.rng.Intn(len(ids))]
			pick[id] = true
			list = append(list, strconv.Itoa(id))
		}
		list = append(list, strconv.Itoa(h.nextID+1000)) // never existed
		h.deleteStmt(tg, `DELETE FROM chunks WHERE id IN (`+strings.Join(list, ", ")+`)`, func(r *rsRow) bool { return pick[r.id] })
		return
	}
	srcs := h.sourceSet(1 + h.rng.Intn(6))
	names := make([]string, len(srcs))
	pick := map[string]bool{}
	for i, s := range srcs {
		names[i] = rsSourceName(s)
		pick[names[i]] = true
	}
	if h.rng.Intn(3) == 0 {
		names = append(names, "nosuchsource")
	}
	h.deleteStmt(tg, `DELETE FROM chunks WHERE source_id IN (`+rsInList(names)+`)`, func(r *rsRow) bool { return pick[r.source] })
	// Importers re-add what they just removed, with a new load.
	for _, s := range srcs {
		if h.rng.Intn(2) == 0 {
			rows, _ := h.newRows(s, 3+h.rng.Intn(5))
			h.insertRows(tg, rows)
		}
	}
}

func (h *rsHarness) opDeleteMissing(tg *rsTarget) {
	switch h.rng.Intn(3) {
	case 0:
		h.deleteStmt(tg, `DELETE FROM chunks WHERE source_id = 'nosuchsource'`, func(*rsRow) bool { return false })
	case 1:
		h.deleteStmt(tg, `DELETE FROM chunks WHERE id < 0`, func(*rsRow) bool { return false })
	default:
		name := rsSourceName(h.rng.Intn(rsSources))
		h.deleteStmt(tg, fmt.Sprintf(`DELETE FROM chunks WHERE source_id = '%s' AND load_id = 'L-none'`, name), func(*rsRow) bool { return false })
	}
}

// opTouch is the importer's bookkeeping update of unchanged documents.
func (h *rsHarness) opTouch(tg *rsTarget, nSources int) {
	srcs := h.sourceSet(nSources)
	names := make([]string, len(srcs))
	pick := map[string]bool{}
	for i, s := range srcs {
		names[i] = rsSourceName(s)
		pick[names[i]] = true
	}
	h.clock += 1 + h.rng.Intn(40)
	x := h.clock
	if h.rng.Intn(5) == 0 {
		x -= h.rng.Intn(60) // an older stamp: only the rows that are older still change
	}
	y := h.rng.Intn(3000)
	sql := fmt.Sprintf(`UPDATE chunks SET loaded_at = %d, doc_date = %d WHERE source_id IN (%s) AND COALESCE(loaded_at, 0) < %d`, x, y, rsInList(names), x)
	n := h.updateStmt(tg, sql,
		func(r *rsRow) bool {
			l := r.loaded
			if l < 0 {
				l = 0
			}
			return pick[r.source] && l < x
		},
		func(r *rsRow) { r.loaded, r.docDate = x, y })
	h.stats.touches++
	h.stats.touchedRows += n
}

func (h *rsHarness) pickIDs(tg *rsTarget, n int) []int {
	ids := tg.o.sortedIDs()
	var out []int
	for i := 0; i < n && len(ids) > 0; i++ {
		out = append(out, ids[h.rng.Intn(len(ids))])
	}
	return out
}

func (h *rsHarness) opContent(tg *rsTarget) {
	text := h.text()
	if h.rng.Intn(3) == 0 {
		src := rsSourceName(h.rng.Intn(rsSources))
		h.updateStmt(tg, fmt.Sprintf(`UPDATE chunks SET content = '%s' WHERE source_id = '%s'`, text, src),
			func(r *rsRow) bool { return r.source == src }, func(r *rsRow) { r.content = text })
	} else {
		ids := h.pickIDs(tg, 1+h.rng.Intn(3))
		set := map[int]bool{}
		list := make([]string, 0, len(ids))
		for _, id := range ids {
			if !set[id] {
				list = append(list, strconv.Itoa(id))
			}
			set[id] = true
		}
		h.clock++
		stamp := h.clock
		h.updateStmt(tg, fmt.Sprintf(`UPDATE chunks SET content = '%s', loaded_at = %d WHERE id IN (%s)`, text, stamp, strings.Join(list, ", ")),
			func(r *rsRow) bool { return set[r.id] }, func(r *rsRow) { r.content, r.loaded = text, stamp })
	}
	h.stats.contentUpdates++
}

func (h *rsHarness) opEmbedding(tg *rsTarget) {
	v := h.vec()
	if h.rng.Intn(3) == 0 {
		src := rsSourceName(h.rng.Intn(rsSources))
		h.updateStmt(tg, fmt.Sprintf(`UPDATE chunks SET embedding = '%s' WHERE source_id = '%s'`, retrievalVecLiteral(v), src),
			func(r *rsRow) bool { return r.source == src }, func(r *rsRow) { r.vec = v })
	} else {
		ids := h.pickIDs(tg, 1+h.rng.Intn(4))
		set := map[int]bool{}
		list := make([]string, 0, len(ids))
		for _, id := range ids {
			if !set[id] {
				list = append(list, strconv.Itoa(id))
			}
			set[id] = true
		}
		h.updateStmt(tg, fmt.Sprintf(`UPDATE chunks SET embedding = '%s' WHERE id IN (%s)`, retrievalVecLiteral(v), strings.Join(list, ", ")),
			func(r *rsRow) bool { return set[r.id] }, func(r *rsRow) { r.vec = v })
	}
	h.stats.embeddingUpdates++
	switch h.rng.Intn(3) {
	case 0:
		h.probeHNSW(tg, v)
	default:
		// Probe after the next DELETE instead: the update and the removal then fall
		// into one window of the change log, and the graph has to map the updated
		// rows through the shifted positions.
		h.pendingProbe = v
	}
}

// probeHNSW searches the HNSW index for a vector that was just written to some
// rows. The graph (built earlier, followed through removals since) knows those
// rows only through the update log, which makes the search merge them exactly; a
// row that carries the query vector must therefore come back first, at distance
// zero, however the positions shifted meanwhile. Comparing recall alone would not
// notice: on a table this small the traversal reaches nearly every row anyway.
func (h *rsHarness) probeHNSW(tg *rsTarget, v []float64) {
	carriers := map[int]bool{}
	for id, r := range tg.o {
		if len(r.vec) > 0 && &r.vec[0] == &v[0] {
			carriers[id] = true
		}
	}
	if len(carriers) == 0 {
		return
	}
	rs := h.must(tg.db, h.tenant, fmt.Sprintf(`SELECT id, _vec_distance FROM VEC_SEARCH('chunks', 'embedding', '%s', 3, 'cosine', 'hnsw')`, retrievalVecLiteral(v)))
	if len(rs.Rows) == 0 || !carriers[rsInt(rs.Rows[0]["id"])] || math.Abs(rsFloat(rs.Rows[0]["_vec_distance"])) > 1e-9 {
		h.fatalf("the HNSW search for a vector just written to %d rows returned %v first, not one of them", len(carriers), rs.Rows)
	}
}

func (h *rsHarness) opNoop(tg *rsTarget) {
	switch h.rng.Intn(3) {
	case 0:
		src := rsSourceName(h.rng.Intn(rsSources))
		h.updateStmt(tg, fmt.Sprintf(`UPDATE chunks SET loaded_at = loaded_at, content = content WHERE source_id = '%s'`, src),
			func(r *rsRow) bool { return r.source == src }, func(*rsRow) {})
	case 1:
		h.updateStmt(tg, `UPDATE chunks SET loaded_at = 5 WHERE id < 0`, func(*rsRow) bool { return false }, func(*rsRow) {})
	default:
		src := rsSourceName(h.rng.Intn(rsSources))
		h.updateStmt(tg, fmt.Sprintf(`UPDATE chunks SET embed_model = embed_model WHERE source_id = '%s'`, src),
			func(r *rsRow) bool { return r.source == src }, func(*rsRow) {})
	}
	h.stats.noops++
}

// opFail runs a statement that must fail after it already changed some rows (or
// inserted some of its rows), and checks nothing of it stays.
func (h *rsHarness) opFail(tg *rsTarget) {
	ids := tg.o.sortedIDs()
	victim := ids[len(ids)*2/3]
	var sql string
	switch k := h.rng.Intn(5); {
	case k == 0:
		sql = fmt.Sprintf(`UPDATE chunks SET content = 'ghost phantom', embedding = '%s', loaded_at = 1 / (id - %d)`, retrievalVecLiteral(h.vec()), victim)
	case k == 1:
		sql = fmt.Sprintf(`DELETE FROM chunks WHERE 1 / (id - %d) > 0`, victim)
	case k == 2:
		sql = fmt.Sprintf(`DELETE FROM chunks WHERE source_id = '%s' AND 1 / (id - %d) > 0`, tg.o[victim].source, victim)
	case k == 3 && h.variant == 1:
		// a duplicate key on the unique index, after two good rows of the same statement
		rows, _ := h.newRows(h.rng.Intn(rsSources), 2)
		dup := *rows[0]
		dup.id = victim
		rows = append(rows, &dup)
		vals := make([]string, len(rows))
		for i, r := range rows {
			vals[i] = r.values()
		}
		sql = `INSERT INTO chunks VALUES ` + strings.Join(vals, ", ")
	case k == 4 && h.variant == 1:
		// an UPDATE that makes an id collide
		other := ids[(len(ids)/3)%len(ids)]
		if other == victim {
			other = ids[0]
		}
		sql = fmt.Sprintf(`UPDATE chunks SET id = %d WHERE id = %d`, other, victim)
	default:
		// the last of three rows has an unparsable vector
		rows, _ := h.newRows(h.rng.Intn(rsSources), 3)
		vals := make([]string, len(rows))
		for i, r := range rows {
			vals[i] = r.values()
		}
		vals[2] = strings.Replace(rows[2].values(), retrievalVecLiteral(rows[2].vec), "[1, 2", 1)
		sql = `INSERT INTO chunks VALUES ` + strings.Join(vals, ", ")
	}
	if _, err := rsTry(tg.db, h.tenant, sql); err == nil {
		h.fatalf("the failing statement succeeded: %.300s", sql)
	}
	h.stats.failed++
}

// ---------------------------------------------------------------------------
// the side table: scalar columns only, an index, about 350 rows. A table with a
// vector column never gets the row twins that let a transaction take its private
// copy without copying (they need scalar columns and 256 rows), and the index
// twins with them; this table is what exercises the interplay of the in-place
// renumbering of an index by DELETE with the twins that consecutive transactions
// hand over to each other. It takes part in the same statements, transactions,
// savepoints, Syncs and reopens as the chunk table.

const rsCreateSide = `CREATE TABLE side (id INT, grp TEXT, n INT, note TEXT)`

type rsSideRow struct {
	id   int
	grp  string
	n    int
	note string
}

type rsSide map[int]*rsSideRow

func (s rsSide) clone() rsSide {
	c := make(rsSide, len(s))
	for id, r := range s {
		c[id] = r
	}
	return c
}

func (s rsSide) sortedIDs() []int {
	ids := make([]int, 0, len(s))
	for id := range s {
		ids = append(ids, id)
	}
	sort.Ints(ids)
	return ids
}

func (r *rsSideRow) values() string {
	return fmt.Sprintf("(%d, '%s', %d, '%s')", r.id, r.grp, r.n, r.note)
}

func (h *rsHarness) sideRows(grp string, n int) []*rsSideRow {
	rows := make([]*rsSideRow, n)
	for i := range rows {
		rows[i] = &rsSideRow{id: h.nextSide, grp: grp, n: h.rng.Intn(1000), note: h.text()}
		h.nextSide++
	}
	return rows
}

func (h *rsHarness) sideInsert(tg *rsTarget, rows []*rsSideRow) {
	h.t.Helper()
	vals := make([]string, len(rows))
	for i, r := range rows {
		vals[i] = r.values()
	}
	h.must(tg.db, h.tenant, `INSERT INTO side VALUES `+strings.Join(vals, ", "))
	for _, r := range rows {
		tg.s[r.id] = r
	}
	h.record(tg)
}

func (h *rsHarness) sideDelete(tg *rsTarget, sql string, match func(r *rsSideRow) bool) {
	h.t.Helper()
	rs := h.must(tg.db, h.tenant, sql)
	want := 0
	for id, r := range tg.s {
		if match(r) {
			delete(tg.s, id)
			want++
		}
	}
	if got := rsInt(rs.Rows[0]["deleted"]); got != want {
		h.fatalf("%s deleted %d rows, the oracle says %d", sql, got, want)
	}
	h.record(tg)
}

// opSide runs one statement on the side table; light ones neither delete nor fail.
func (h *rsHarness) opSide(tg *rsTarget, light bool) {
	if len(tg.s) < 300 {
		h.sideInsert(tg, h.sideRows(fmt.Sprintf("g%d", h.rng.Intn(30)), 60))
	}
	if light {
		if h.rng.Intn(2) == 0 {
			h.sideInsert(tg, h.sideRows(fmt.Sprintf("g%d", h.rng.Intn(30)), 1+h.rng.Intn(5)))
			return
		}
		grp := fmt.Sprintf("g%d", h.rng.Intn(30))
		h.must(tg.db, h.tenant, fmt.Sprintf(`UPDATE side SET n = n + 1 WHERE grp = '%s'`, grp))
		for _, id := range tg.s.sortedIDs() {
			if r := tg.s[id]; r.grp == grp {
				cp := *r
				cp.n++
				tg.s[id] = &cp
			}
		}
		h.record(tg)
		return
	}
	grp := fmt.Sprintf("g%d", h.rng.Intn(30))
	switch k := h.rng.Intn(10); {
	case k < 4: // replace a group, the importer's cycle
		rows := h.sideRows(grp, 6+h.rng.Intn(9))
		first := rows[0].id
		h.sideInsert(tg, rows)
		h.sideDelete(tg, fmt.Sprintf(`DELETE FROM side WHERE grp = '%s' AND id < %d`, grp, first),
			func(r *rsSideRow) bool { return r.grp == grp && r.id < first })
	case k < 6: // delete by ids
		ids := tg.s.sortedIDs()
		pick := map[int]bool{}
		var list []string
		for i := 0; i < 1+h.rng.Intn(20); i++ {
			id := ids[h.rng.Intn(len(ids))]
			if !pick[id] {
				list = append(list, strconv.Itoa(id))
			}
			pick[id] = true
		}
		h.sideDelete(tg, `DELETE FROM side WHERE id IN (`+strings.Join(list, ", ")+`)`, func(r *rsSideRow) bool { return pick[r.id] })
	case k < 8: // an update that leaves the index alone
		g2 := fmt.Sprintf("g%d", h.rng.Intn(30))
		h.must(tg.db, h.tenant, fmt.Sprintf(`UPDATE side SET n = n + 1 WHERE grp IN ('%s', '%s')`, grp, g2))
		for _, id := range tg.s.sortedIDs() {
			if r := tg.s[id]; r.grp == grp || r.grp == g2 {
				cp := *r
				cp.n++
				tg.s[id] = &cp
			}
		}
		h.record(tg)
	case k < 9: // delete a whole group
		h.sideDelete(tg, fmt.Sprintf(`DELETE FROM side WHERE grp = '%s'`, grp), func(r *rsSideRow) bool { return r.grp == grp })
	default: // a statement that fails after it deleted some rows
		ids := tg.s.sortedIDs()
		victim := ids[len(ids)/2]
		if _, err := rsTry(tg.db, h.tenant, fmt.Sprintf(`DELETE FROM side WHERE 1 / (id - %d) > 0`, victim)); err == nil {
			h.fatalf("the failing DELETE of the side table succeeded")
		}
		h.stats.failed++
	}
}

// rsCheckSide compares the side table with its oracle, through a scan and
// through its index.
func rsCheckSide(label string, db *storage.DB, tenant string, s rsSide, fail func(format string, args ...any)) {
	must := func(sql string) *ResultSet {
		rs, err := rsTry(db, tenant, sql)
		if err != nil {
			fail("%s: %.300s: %v", label, sql, err)
		}
		return rs
	}
	rs := must(`SELECT id, grp, n, note FROM side`)
	if len(rs.Rows) != len(s) {
		fail("%s: the side table has %d rows, the oracle %d", label, len(rs.Rows), len(s))
	}
	seen := map[int]bool{}
	for _, r := range rs.Rows {
		id := rsInt(r["id"])
		w := s[id]
		if w == nil || seen[id] || r["grp"] != w.grp || rsInt(r["n"]) != w.n || r["note"] != w.note {
			fail("%s: side row %d is %v, the oracle has %v", label, id, r, w)
		}
		seen[id] = true
	}
	for g := 0; g <= 30; g++ {
		grp := fmt.Sprintf("g%d", g)
		want := []int{}
		for _, id := range s.sortedIDs() {
			if s[id].grp == grp {
				want = append(want, id)
			}
		}
		for _, sql := range []string{
			fmt.Sprintf(`SELECT id FROM side WHERE grp = '%s'`, grp),
			fmt.Sprintf(`SELECT id FROM side WHERE UPPER(grp) = '%s'`, strings.ToUpper(grp)),
		} {
			got := rsIDs(must(sql))
			sort.Ints(got)
			if !reflect.DeepEqual(got, want) {
				fail("%s: %s returned %v, the oracle has %v", label, sql, rsHead(got), rsHead(want))
			}
		}
	}
}

func (h *rsHarness) verifySide(label string, db *storage.DB, s rsSide) {
	h.t.Helper()
	rsCheckSide(h.tag()+": "+label, db, h.tenant, s, func(format string, args ...any) {
		h.t.Helper()
		h.t.Fatalf(format, args...)
	})
}

// ---------------------------------------------------------------------------
// transactions, the way the driver runs them: a shadow database that shares the
// live tables until it writes one, merged back with ApplyWALChanges at COMMIT.

type rsSave struct {
	shadow *storage.DB
	o      rsOracle
	s      rsSide
}

// runTx runs one transaction; sideOnly ones touch nothing but the side table.
func (h *rsHarness) runTx(sideOnly bool) {
	live := h.db
	base, shadow, release := func() (*storage.DB, *storage.DB, func()) {
		live.LockContentForRead()
		defer live.UnlockContentForRead()
		return live.SnapshotForTxShared()
	}()
	tg := &rsTarget{db: shadow, o: h.oracle.clone(), s: h.side.clone()}
	if h.rng.Intn(10) < 7 {
		// Half of these transactions only insert and update: a table whose log holds
		// no removal is the one a twin can be handed on for.
		h.opSide(tg, h.rng.Intn(2) == 0)
	}
	var saves []rsSave
	steps := 2 + h.rng.Intn(6)
	for i := 0; i < steps; i++ {
		if sideOnly {
			h.opSide(tg, h.rng.Intn(3) != 0)
			continue
		}
		switch k := h.rng.Intn(10); {
		case k < 4:
			h.randomOp(tg, true)
		case k < 6 && len(saves) < 3:
			saved := func() *storage.DB {
				live.LockContentForRead()
				defer live.UnlockContentForRead()
				return tg.db.SnapshotForWriteTx()
			}()
			saves = append(saves, rsSave{shadow: saved, o: tg.o.clone(), s: tg.s.clone()})
			h.stats.savepoints++
		case k < 8 && len(saves) > 0:
			at := h.rng.Intn(len(saves))
			tg.db = saves[at].shadow.SnapshotForWriteTx()
			tg.o = saves[at].o.clone()
			tg.s = saves[at].s.clone()
			saves = saves[:at+1]
			h.stats.savepointRollbacks++
		case k == 8:
			h.checkLive(tg.db, tg.o, "inside a transaction")
		default:
			if h.rng.Intn(3) != 0 {
				h.opSide(tg, false)
			} else {
				h.opFail(tg)
			}
		}
	}
	if sideOnly && h.rng.Intn(5) != 0 || !sideOnly && h.rng.Intn(10) < 6 {
		err := func() error {
			live.LockContentForWrite()
			defer live.UnlockContentForWrite() // also when a verification panics: the readers must be able to finish
			return live.ApplyWALChanges(storage.CollectWALChanges(base, tg.db))
		}()
		if err != nil {
			h.fatalf("commit: %v", err)
		}
		h.oracle = tg.o
		h.side = tg.s
		h.record(&rsTarget{db: h.db, o: h.oracle, s: h.side})
		h.stats.txCommits++
	} else {
		h.stats.txRollbacks++
	}
	release()
}

// randomOp runs one statement of the ordinary mix against tg.
func (h *rsHarness) randomOp(tg *rsTarget, inTx bool) {
	h.stats.statements++
	switch k := h.rng.Intn(100); {
	case k < 38:
		h.opReplace(tg, h.rng.Intn(rsSources))
	case k < 46:
		h.opDeleteIn(tg)
	case k < 50:
		h.opDeleteMissing(tg)
	case k < 62:
		h.opTouch(tg, 3+h.rng.Intn(43)) // about 20 to 300 rows
	case k < 72:
		h.opContent(tg)
	case k < 80:
		h.opEmbedding(tg)
	case k < 84:
		h.opNoop(tg)
	case k < 92:
		h.opSide(tg, false)
	default:
		h.opFail(tg)
	}
	if h.rng.Intn(5) == 0 {
		h.checkLive(tg.db, tg.o, "after a statement")
	}
}

// capPhase crosses the bound of the retained UPDATE history (maxRowUpdateDeltas)
// with no search in between, once with a removal before the touches so that the
// fold has to forget a removal, and checks the answers afterwards.
func (h *rsHarness) capPhase(withRemoval bool) {
	tg := &rsTarget{db: h.db, o: h.oracle, s: h.side}
	h.stats.capPhases++
	// Grow the table to above 1100 rows so that 70 touches of every row cross 65536.
	for len(tg.o) < 1150 {
		rows, _ := h.newRows(h.rng.Intn(rsSources), 100)
		h.insertRows(tg, rows)
	}
	h.checkLive(tg.db, tg.o, "before the cap phase") // a consumer is stamped here
	if withRemoval {
		ids := h.pickIDs(tg, 5)
		set := map[int]bool{}
		list := make([]string, 0, len(ids))
		for _, id := range ids {
			if !set[id] {
				list = append(list, strconv.Itoa(id))
			}
			set[id] = true
		}
		h.deleteStmt(tg, `DELETE FROM chunks WHERE id IN (`+strings.Join(list, ", ")+`)`, func(r *rsRow) bool { return set[r.id] })
	}
	n := len(tg.o)
	touches := 70
	if withRemoval {
		touches = 68
	}
	sv0 := h.structVersion()
	lastOdd := 0
	for i := 0; i < touches; i++ {
		h.clock++
		x := h.clock
		if i%2 == 0 {
			h.must(tg.db, h.tenant, fmt.Sprintf(`UPDATE chunks SET loaded_at = %d`, x))
		} else {
			h.must(tg.db, h.tenant, fmt.Sprintf(`UPDATE chunks SET loaded_at = %d, doc_date = %d WHERE COALESCE(loaded_at, 0) < %d`, x, x%3000, x+1))
			lastOdd = x
		}
		h.stats.touchedRows += n
	}
	// The UPDATE history is bounded (maxRowUpdateDeltas, 65536, in the storage
	// package): the touches must have gone past it, or the fold was not exercised.
	if got := h.structVersion() - sv0; got <= 65536 {
		h.fatalf("the touches of the cap phase logged %d row events, not enough to fold the UPDATE history", got)
	}
	// Every touch overwrote loaded_at of every row (the second kind matches every
	// row too, because the stamp grows) and the odd ones doc_date as well. The
	// oracle is brought up to date once.
	lastX := h.clock
	for _, id := range tg.o.sortedIDs() {
		tg.o.update(id, func(r *rsRow) {
			r.loaded = lastX
			r.docDate = lastOdd % 3000
		})
	}
	h.record(tg)
	h.checkLive(tg.db, tg.o, "after the cap phase")
	// A rewrite of a row after the fold must still be picked up.
	h.opContent(tg)
	h.checkLive(tg.db, tg.o, "after the cap phase and a content update")
	// Shrink again so that the rest of the run stays small.
	if len(tg.o) > 800 {
		for _, s := range h.sourceSet(rsSources / 2) {
			name := rsSourceName(s)
			h.deleteStmt(tg, fmt.Sprintf(`DELETE FROM chunks WHERE source_id = '%s'`, name), func(r *rsRow) bool { return r.source == name })
		}
	}
	h.oracle = tg.o
}

// ---------------------------------------------------------------------------
// checks on a live database against the oracle, on the writer's goroutine

// rsHist is the state of both tables after one statement.
type rsHist struct {
	o rsOracle
	s rsSide
}

// record remembers the oracle of the live database under the versions of its two
// tables.
func (h *rsHarness) record(tg *rsTarget) {
	if tg.db != h.db {
		return
	}
	var key [2]int
	func() {
		h.db.LockContentForRead()
		defer h.db.UnlockContentForRead()
		for i, name := range []string{"chunks", "side"} {
			tbl, err := h.db.Get(h.tenant, name)
			if err != nil {
				h.fatalf("Get: %v", err)
			}
			key[i] = tbl.Version
		}
	}()
	h.histMu.Lock()
	defer h.histMu.Unlock()
	if _, ok := h.history[key]; !ok {
		h.histOrder = append(h.histOrder, key)
		if len(h.histOrder) > rsHistory {
			delete(h.history, h.histOrder[0])
			h.histOrder = h.histOrder[1:]
		}
	}
	h.history[key] = rsHist{o: tg.o.clone(), s: tg.s.clone()}
}

func (h *rsHarness) resetHistory() {
	h.histMu.Lock()
	h.history, h.histOrder = map[[2]int]rsHist{}, nil
	h.histMu.Unlock()
}

func (h *rsHarness) structVersion() int {
	h.db.LockContentForRead()
	defer h.db.UnlockContentForRead()
	tbl, err := h.db.Get(h.tenant, "chunks")
	if err != nil {
		h.fatalf("Get: %v", err)
	}
	return tbl.StructVersion()
}

func rsCheckLive(label string, db *storage.DB, tenant string, o rsOracle, r *rand.Rand, fail func(format string, args ...any)) {
	must := func(sql string) *ResultSet {
		rs, err := rsTry(db, tenant, sql)
		if err != nil {
			fail("%s: %.300s: %v", label, sql, err)
		}
		return rs
	}
	var term string
	switch r.Intn(3) {
	case 0:
		term = fmt.Sprintf("w%d", r.Intn(24))
	case 1:
		term = fmt.Sprintf("tag%d", r.Intn(6))
	default:
		term = fmt.Sprintf("rare%d", r.Intn(400))
	}
	// Full-text: exactly the rows that contain the term.
	rs := must(fmt.Sprintf(`SELECT id, content, _fts_score FROM FTS_SEARCH('chunks', '%s', 100000, 'content')`, term))
	got := make([]int, 0, len(rs.Rows))
	last := math.Inf(1)
	for _, r := range rs.Rows {
		id := rsInt(r["id"])
		got = append(got, id)
		s := rsFloat(r["_fts_score"])
		if math.IsNaN(s) || s <= 0 || s > last {
			fail("%s: FTS_SEARCH(%s) score %v after %v", label, term, s, last)
		}
		last = s
		if row := o[id]; row == nil || row.content != r["content"] {
			fail("%s: FTS_SEARCH(%s) returned row %d with content %q, the oracle has %v", label, term, id, r["content"], row)
		}
	}
	want := []int{}
	for _, id := range o.sortedIDs() {
		if rsHasToken(o[id].content, term) {
			want = append(want, id)
		}
	}
	sort.Ints(got)
	if !reflect.DeepEqual(got, want) {
		fail("%s: FTS_SEARCH(%s) returned %d rows %v, the oracle has %d rows %v", label, term, len(got), rsHead(got), len(want), rsHead(want))
	}

	// Exact vector search: every row, with the distance of the stored vector.
	q := rsRandVec(r)
	rs = must(fmt.Sprintf(`SELECT id, _vec_distance FROM VEC_SEARCH('chunks', 'embedding', '%s', 100000, 'cosine')`, retrievalVecLiteral(q)))
	if len(rs.Rows) != len(o) {
		count := must(`SELECT COUNT(*) AS c FROM chunks`).Rows[0]["c"]
		fail("%s: VEC_SEARCH returned %d rows, the oracle has %d (SELECT COUNT(*) says %v)", label, len(rs.Rows), len(o), count)
	}
	prev := math.Inf(-1)
	seen := make(map[int]bool, len(rs.Rows))
	for _, r := range rs.Rows {
		id, d := rsInt(r["id"]), rsFloat(r["_vec_distance"])
		row := o[id]
		if row == nil || seen[id] {
			fail("%s: VEC_SEARCH returned row %d (in the oracle: %v, repeated: %v)", label, id, row != nil, seen[id])
		}
		seen[id] = true
		if want := rsCosineDistance(row.vec, q); math.Abs(d-want) > 1e-9 || d < prev {
			fail("%s: VEC_SEARCH row %d distance %v, the oracle's vector gives %v (previous %v)", label, id, d, want, prev)
		}
		prev = d
	}

	// Approximate search: whatever it returns exists and reports the true distance.
	for _, index := range []string{"hnsw", "flat"} {
		rs = must(fmt.Sprintf(`SELECT id, _vec_distance FROM VEC_SEARCH('chunks', 'embedding', '%s', 8, 'cosine', '%s')`, retrievalVecLiteral(q), index))
		seen = map[int]bool{}
		for _, r := range rs.Rows {
			id, d := rsInt(r["id"]), rsFloat(r["_vec_distance"])
			row := o[id]
			if row == nil || seen[id] {
				fail("%s: VEC_SEARCH %s returned row %d (in the oracle: %v, repeated: %v)", label, index, id, row != nil, seen[id])
			}
			seen[id] = true
			if want := rsCosineDistance(row.vec, q); math.Abs(d-want) > 1e-9 {
				fail("%s: VEC_SEARCH %s row %d distance %v, the oracle's vector gives %v", label, index, id, d, want)
			}
		}
		rs = must(fmt.Sprintf(`SELECT id, content, _rrf_rank, _rrf_score FROM HYBRID_SEARCH('chunks', 'embedding', 'content', '%s OR w%d', '%s', 6, '{"key_columns":["id"],"candidate_k":24,"rrf_k":60,"metric":"cosine","index":"%s"}')`, term, r.Intn(24), retrievalVecLiteral(q), index))
		seen = map[int]bool{}
		for _, r := range rs.Rows {
			id := rsInt(r["id"])
			if row := o[id]; row == nil || seen[id] || row.content != r["content"] {
				fail("%s: HYBRID_SEARCH %s returned row %d with content %q (oracle: %v, repeated: %v)", label, index, id, r["content"], row, seen[id])
			}
			seen[id] = true
		}
	}
}

// checkLive checks db against the oracle (see rsCheckLive) on the test goroutine.
func (h *rsHarness) checkLive(db *storage.DB, o rsOracle, label string) {
	h.t.Helper()
	h.stats.checks++
	rsCheckLive(h.tag()+": "+label, db, h.tenant, o, h.rng, func(format string, args ...any) {
		h.t.Helper()
		h.t.Fatalf(format, args...)
	})
}

func (h *rsHarness) verifyTable(label string, db *storage.DB, o rsOracle) {
	h.t.Helper()
	rsCheckTable(h.tag()+": "+label, db, h.tenant, o, func(format string, args ...any) {
		h.t.Helper()
		h.t.Fatalf(format, args...)
	})
}

func rsRandVec(r *rand.Rand) []float64 {
	v := make([]float64, rsDims)
	for i := range v {
		v[i] = float64(r.Intn(2001)-1000) / 1000
	}
	return v
}

func rsHead(ids []int) []int {
	if len(ids) > 12 {
		return ids[:12]
	}
	return ids
}

// ---------------------------------------------------------------------------
// background goroutines: two that Sync, three readers

func (h *rsHarness) startBackground(db *storage.DB, readers int) (stop func()) {
	done := make(chan struct{})
	var wg sync.WaitGroup
	for s := 0; s < 2; s++ {
		wg.Add(1)
		go func(s int) {
			defer wg.Done()
			r := rand.New(rand.NewSource(h.seed*7919 + int64(s)))
			lastCopy := time.Now()
			for {
				select {
				case <-done:
					return
				default:
				}
				h.gate.RLock()
				err := db.Sync()
				h.gate.RUnlock()
				if err != nil {
					h.fail("Sync: %v", err)
					return
				}
				h.stats.syncs.Add(1)
				if s == 0 && time.Since(lastCopy) > 25*time.Millisecond {
					h.checkCopy(r)
					lastCopy = time.Now()
				}
				time.Sleep(time.Duration(r.Intn(2500)) * time.Microsecond)
			}
		}(s)
	}
	for i := 0; i < readers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			r := rand.New(rand.NewSource(h.seed*104729 + int64(i)))
			for {
				select {
				case <-done:
					return
				default:
				}
				h.readOnce(db, r)
				h.stats.reads.Add(1)
				time.Sleep(time.Duration(r.Intn(300)) * time.Microsecond)
			}
		}(i)
	}
	var once sync.Once
	return func() {
		once.Do(func() {
			close(done)
			// A panic inside the storage code can leave one of its locks held for ever
			// (the twin verification panics in the middle of ApplyWALChanges); do not
			// wait for goroutines that are stuck on it, the panic has to be reported.
			finished := make(chan struct{})
			go func() { wg.Wait(); close(finished) }()
			select {
			case <-finished:
			case <-time.After(10 * time.Second):
			}
		})
	}
}

// checkCopy copies the storage directory while the writer runs (no Sync runs
// meanwhile: that is what the gate is for, the files only change in a Sync),
// opens the copy as a crashed process's files would be opened and requires it to
// hold exactly one of the states the writer went through, the one its table
// version names, and to answer searches like that state. A snapshot that mixed
// rows of different instants, a version recorded for the wrong rows, or a search
// index image ahead of the rows would show here and nowhere else, because the
// copies of crashPoint are taken while nothing else is writing.
func (h *rsHarness) checkCopy(r *rand.Rand) {
	h.gate.Lock()
	err := os.RemoveAll(h.chkDir)
	if err == nil {
		err = rsCopyTree(h.dir, h.chkDir)
	}
	h.gate.Unlock()
	if err != nil {
		h.fail("copy of the storage directory: %v", err)
		return
	}
	defer func() {
		if p := recover(); p != nil {
			if msg, ok := p.(rsFatal); ok {
				h.fail("copy taken during the workload: %s", string(msg))
				return
			}
			panic(p)
		}
	}()
	fail := func(format string, args ...any) { panic(rsFatal(fmt.Sprintf(format, args...))) }
	db, err := storage.OpenDB(h.cfg.storage(h.chkDir))
	if err != nil {
		fail("open the copy: %v", err)
	}
	defer db.Close()
	tbl, err := db.Get(h.tenant, "chunks")
	if err != nil {
		fail("the copy has no table: %v", err)
	}
	side, err := db.Get(h.tenant, "side")
	if err != nil {
		fail("the copy has no side table: %v", err)
	}
	key := [2]int{tbl.Version, side.Version}
	h.histMu.Lock()
	state, known := h.history[key]
	h.histMu.Unlock()
	if !known {
		h.stats.copyUnmatched.Add(1) // taken between a statement and its record, or older than the history
		return
	}
	label := fmt.Sprintf("%s: the copy at table versions %v", h.tag(), key)
	rsCheckTable(label, db, h.tenant, state.o, fail)
	rsCheckSide(label, db, h.tenant, state.s, fail)
	rsCheckSourceIndex(label, db, h.tenant, state.o, r, fail)
	if r.Intn(2) == 0 {
		rsCheckLive(label, db, h.tenant, state.o, r, fail)
	}
	h.stats.copyChecked.Add(1)
}

// rsCheckSourceIndex asks for the rows of some sources through the index on
// (source_id, chunk_idx) and through a predicate no index can serve.
func rsCheckSourceIndex(label string, db *storage.DB, tenant string, o rsOracle, r *rand.Rand, fail func(format string, args ...any)) {
	for i := 0; i < 12; i++ {
		name := rsSourceName(r.Intn(rsSources))
		want := []int{}
		for _, id := range o.sortedIDs() {
			if o[id].source == name {
				want = append(want, id)
			}
		}
		for _, sql := range []string{
			fmt.Sprintf(`SELECT id FROM chunks WHERE source_id = '%s'`, name),
			fmt.Sprintf(`SELECT id FROM chunks WHERE UPPER(source_id) = '%s'`, strings.ToUpper(name)),
		} {
			rs, err := rsTry(db, tenant, sql)
			if err != nil {
				fail("%s: %.300s: %v", label, sql, err)
			}
			got := rsIDs(rs)
			sort.Ints(got)
			if !reflect.DeepEqual(got, want) {
				fail("%s: %s returned %v, the oracle has %v", label, sql, rsHead(got), rsHead(want))
			}
		}
	}
}

func rsCopyTree(src, dst string) error {
	return filepath.Walk(src, func(path string, info os.FileInfo, err error) error {
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
}

func (h *rsHarness) checkMeta(what string, id int, source any, idx any) bool {
	m, ok := h.meta.Load(id)
	if !ok {
		h.fail("%s returned id %d that was never inserted", what, id)
		return false
	}
	meta := m.(rsMeta)
	if source != nil && source != meta.source || idx != nil && rsInt(idx) != meta.idx {
		h.fail("%s returned id %d as source %v chunk %v, it was inserted as %v", what, id, source, idx, meta)
		return false
	}
	return true
}

// readOnce runs one random search and checks what needs no oracle.
func (h *rsHarness) readOnce(db *storage.DB, r *rand.Rand) {
	vec := make([]float64, rsDims)
	for i := range vec {
		vec[i] = float64(r.Intn(2001)-1000) / 1000
	}
	lit := retrievalVecLiteral(vec)
	switch k := r.Intn(8); {
	case k < 3:
		var q string
		var terms []string
		var phrase bool
		switch r.Intn(3) {
		case 0:
			terms = []string{fmt.Sprintf("w%d", r.Intn(24))}
			q = terms[0]
		case 1:
			terms = []string{fmt.Sprintf("w%d", r.Intn(24)), fmt.Sprintf("tag%d", r.Intn(6))}
			q = terms[0] + " OR " + terms[1]
		default:
			terms = []string{fmt.Sprintf("w%d", r.Intn(24)), fmt.Sprintf("w%d", r.Intn(24))}
			q = `"` + terms[0] + " " + terms[1] + `"`
			phrase = true
		}
		limit := []int{8, 40, 100000}[r.Intn(3)]
		sql := fmt.Sprintf(`SELECT id, source_id, chunk_idx, content, _fts_score, _fts_rank FROM FTS_SEARCH('chunks', '%s', %d, 'content')`, strings.ReplaceAll(q, "'", ""), limit)
		rs, err := rsTry(db, h.tenant, sql)
		if err != nil {
			h.fail("reader: %s: %v", sql, err)
			return
		}
		seen := map[int]bool{}
		last := math.Inf(1)
		for i, row := range rs.Rows {
			id := rsInt(row["id"])
			s := rsFloat(row["_fts_score"])
			content, _ := row["content"].(string)
			switch {
			case seen[id]:
				h.fail("reader: %s returned id %d twice", sql, id)
				return
			case math.IsNaN(s) || math.IsInf(s, 0) || s <= 0 || s > last:
				h.fail("reader: %s score %v after %v", sql, s, last)
				return
			case rsInt(row["_fts_rank"]) != i+1:
				h.fail("reader: %s rank %v at position %d", sql, row["_fts_rank"], i)
				return
			}
			if !h.checkMeta(sql, id, row["source_id"], row["chunk_idx"]) {
				return
			}
			match := false
			if phrase {
				match = rsHasPhrase(content, terms)
			} else {
				for _, t := range terms {
					match = match || rsHasToken(content, t)
				}
			}
			if !match {
				h.fail("reader: %s returned id %d whose content %q has none of %v", sql, id, content, terms)
				return
			}
			seen[id] = true
			last = s
		}
	case k < 5:
		index := []string{"flat", "hnsw"}[r.Intn(2)]
		sql := fmt.Sprintf(`SELECT id, source_id, chunk_idx, embedding, _vec_distance, _vec_rank FROM VEC_SEARCH('chunks', 'embedding', '%s', %d, 'cosine', '%s')`, lit, 3+r.Intn(10), index)
		rs, err := rsTry(db, h.tenant, sql)
		if err != nil {
			h.fail("reader: %s: %v", sql, err)
			return
		}
		seen := map[int]bool{}
		prev := math.Inf(-1)
		for _, row := range rs.Rows {
			id := rsInt(row["id"])
			d := rsFloat(row["_vec_distance"])
			emb, _ := row["embedding"].([]float64)
			if seen[id] || len(emb) != rsDims || math.IsNaN(d) || d < prev {
				h.fail("reader: %s returned id %d (repeated %v, %d dims) distance %v after %v", sql, id, seen[id], len(emb), d, prev)
				return
			}
			if want := rsCosineDistance(emb, vec); math.Abs(d-want) > 1e-9 {
				h.fail("reader: %s id %d reports distance %v but its vector gives %v", sql, id, d, want)
				return
			}
			if !h.checkMeta(sql, id, row["source_id"], row["chunk_idx"]) {
				return
			}
			seen[id] = true
			prev = d
		}
	default:
		index := []string{"flat", "hnsw"}[r.Intn(2)]
		sql := fmt.Sprintf(`SELECT id, source_id, chunk_idx, content, _rrf_rank, _rrf_score FROM HYBRID_SEARCH('chunks', 'embedding', 'content', 'w%d OR tag%d', '%s', 6, '{"key_columns":["id"],"candidate_k":24,"rrf_k":60,"metric":"cosine","index":"%s"}')`, r.Intn(24), r.Intn(6), lit, index)
		rs, err := rsTry(db, h.tenant, sql)
		if err != nil {
			h.fail("reader: %s: %v", sql, err)
			return
		}
		seen := map[int]bool{}
		last := math.Inf(1)
		for i, row := range rs.Rows {
			id := rsInt(row["id"])
			s := rsFloat(row["_rrf_score"])
			if seen[id] || math.IsNaN(s) || s <= 0 || s > last || rsInt(row["_rrf_rank"]) != i+1 {
				h.fail("reader: %s returned id %d (repeated %v) score %v after %v rank %v at %d", sql, id, seen[id], s, last, row["_rrf_rank"], i)
				return
			}
			if !h.checkMeta(sql, id, row["source_id"], row["chunk_idx"]) {
				return
			}
			seen[id] = true
			last = s
		}
	}
}

// ---------------------------------------------------------------------------
// phases of a run

// phase runs iters iterations of the writer with the background goroutines
// going. crashAt lists iterations after which the storage directory is copied.
func (h *rsHarness) phase(iters int, crashAt map[int]bool, capAt map[int]bool) {
	h.t.Helper()
	stop := h.startBackground(h.db, 3)
	defer stop()
	for i := 0; i < iters; i++ {
		if h.failedF.Load() {
			break
		}
		if capAt[i] {
			h.capPhase(i%2 == 1)
		} else if i%19 == 9 && !h.noTx {
			h.twinBurst()
		} else if h.rng.Intn(100) < 11 && !h.noTx {
			h.runTx(false)
		} else {
			h.randomOp(&rsTarget{db: h.db, o: h.oracle, s: h.side}, false)
		}
		if crashAt[i] {
			h.crashPoint(fmt.Sprintf("crash after iteration %d", i))
		}
	}
	stop()
	if f := h.failures(); f != "" {
		h.fatalf("background goroutines reported:\n  %s", f)
	}
}

// crashPoint quiesces the writer (it is the writer), lets the Syncs in flight
// finish, Syncs once more and copies the storage directory without closing the
// database: what a crash right after that Sync would leave behind.
func (h *rsHarness) crashPoint(label string) {
	h.gate.Lock()
	defer h.gate.Unlock()
	if err := h.db.Sync(); err != nil {
		h.fatalf("%s: Sync: %v", label, err)
	}
	dst := filepath.Join(h.t.TempDir(), "crash")
	copyTree(h.t, h.dir, dst)
	h.crashes = append(h.crashes, rsCrash{dir: dst, oracle: h.oracle.clone(), side: h.side.clone(), label: label})
	h.stats.crashCopies++
}

// ---------------------------------------------------------------------------
// verification of a database against the oracle

type rsQuery struct {
	sql  string
	full bool // a complete ranked list (k above the row count): comparable by id
	kind string
}

func (h *rsHarness) querySet() []rsQuery {
	r := rand.New(rand.NewSource(h.seed ^ 0x5eed))
	vec := func() string {
		v := make([]float64, rsDims)
		for i := range v {
			v[i] = float64(r.Intn(2001)-1000) / 1000
		}
		return retrievalVecLiteral(v)
	}
	terms := []string{
		fmt.Sprintf("w%d", r.Intn(24)),
		fmt.Sprintf("w%d OR w%d", r.Intn(24), r.Intn(24)),
		fmt.Sprintf("w%d AND tag%d", r.Intn(24), r.Intn(6)),
		fmt.Sprintf("rare%d OR w%d", r.Intn(400), r.Intn(24)),
		fmt.Sprintf(`"w%d w%d"`, r.Intn(24), r.Intn(24)),
		"tag*",
		fmt.Sprintf("tag%d OR tag%d OR w%d", r.Intn(6), r.Intn(6), r.Intn(24)),
		"ghost",
	}
	var qs []rsQuery
	for _, term := range terms {
		for _, k := range []int{8, 100000} {
			qs = append(qs, rsQuery{kind: "fts", full: k == 100000,
				sql: fmt.Sprintf(`SELECT id, source_id, content, _fts_score, _fts_rank FROM FTS_SEARCH('chunks', '%s', %d, 'content')`, term, k)})
		}
	}
	for i := 0; i < 3; i++ {
		for _, k := range []int{6, 100000} {
			qs = append(qs, rsQuery{kind: "vec", full: k == 100000,
				sql: fmt.Sprintf(`SELECT id, _vec_distance, _vec_rank FROM VEC_SEARCH('chunks', 'embedding', '%s', %d, 'cosine')`, vec(), k)})
		}
		qs = append(qs, rsQuery{kind: "vec", sql: fmt.Sprintf(`SELECT id, _vec_distance, _vec_rank FROM VEC_SEARCH('chunks', 'embedding', '%s', 5, 'l2')`, vec())})
	}
	for i := 0; i < 4; i++ {
		for _, ck := range []int{24, 100000} {
			qs = append(qs, rsQuery{kind: "hybrid",
				sql: fmt.Sprintf(`SELECT id, _rrf_rank, _rrf_score, _vec_rank, _fts_rank FROM HYBRID_SEARCH('chunks', 'embedding', 'content', '%s', '%s', 6, '{"key_columns":["id"],"candidate_k":%d,"rrf_k":60,"metric":"cosine","index":"flat"}')`, terms[i], vec(), ck)})
		}
	}
	return qs
}

// freshDB builds an in-memory database holding the given rows in the given order.
func (h *rsHarness) freshDB(order []int, o rsOracle) (*storage.DB, string) {
	h.fresh++
	tenant := fmt.Sprintf("%s-fresh%d", h.tenant, h.fresh)
	db := storage.NewDB()
	h.must(db, tenant, rsCreateTable)
	for i := 0; i < len(order); i += 60 {
		end := min(i+60, len(order))
		vals := make([]string, 0, end-i)
		for _, id := range order[i:end] {
			vals = append(vals, o[id].values())
		}
		h.must(db, tenant, `INSERT INTO chunks VALUES `+strings.Join(vals, ", "))
	}
	return db, tenant
}

func (h *rsHarness) dropFresh(db *storage.DB, tenant string) {
	_ = db.Close()
	purgeFTSCachesFor(tenant, "chunks")
	purgeVectorCachesFor(tenant, "chunks")
}

func rsFirstDiff(got, want []Row) string {
	for i := 0; i < max(len(got), len(want)); i++ {
		switch {
		case i >= len(got):
			return fmt.Sprintf("row %d: live has %d rows, fresh has %d; fresh row %v", i, len(got), len(want), want[i])
		case i >= len(want):
			return fmt.Sprintf("row %d: live has %d rows, fresh has %d; live row %v", i, len(got), len(want), got[i])
		case !reflect.DeepEqual(got[i], want[i]):
			return fmt.Sprintf("row %d: live %v, fresh %v", i, got[i], want[i])
		}
	}
	return "identical"
}

func rsCheckTable(label string, db *storage.DB, tenant string, o rsOracle, fail func(format string, args ...any)) {
	must := func(sql string) *ResultSet {
		rs, err := rsTry(db, tenant, sql)
		if err != nil {
			fail("%s: %.300s: %v", label, sql, err)
		}
		return rs
	}
	rs := must(`SELECT id, source_id, source_kind, load_id, loaded_at, doc_date, chunk_idx, content, embedding, embed_model FROM chunks`)
	if len(rs.Rows) != len(o) {
		fail("%s: the table has %d rows, the oracle %d", label, len(rs.Rows), len(o))
	}
	seen := make(map[int]bool, len(o))
	for _, r := range rs.Rows {
		id := rsInt(r["id"])
		w := o[id]
		if w == nil || seen[id] {
			fail("%s: row %d is in the table (oracle has it: %v, repeated: %v)", label, id, w != nil, seen[id])
		}
		seen[id] = true
		loaded := -1
		if r["loaded_at"] != nil {
			loaded = rsInt(r["loaded_at"])
		}
		emb, _ := r["embedding"].([]float64)
		if r["source_id"] != w.source || r["source_kind"] != w.kind || r["load_id"] != w.load || loaded != w.loaded ||
			rsInt(r["doc_date"]) != w.docDate || rsInt(r["chunk_idx"]) != w.idx || r["content"] != w.content ||
			!reflect.DeepEqual(emb, w.vec) || r["embed_model"] != w.model {
			fail("%s: row %d is %v in the table, the oracle has %+v", label, id, r, *w)
		}
	}
}

func rsIDs(rs *ResultSet) []int {
	ids := make([]int, 0, len(rs.Rows))
	for _, r := range rs.Rows {
		ids = append(ids, rsInt(r["id"]))
	}
	return ids
}

// verifyIndexes asks for the same rows through an index and through a predicate
// the indexes cannot serve, and compares both with the oracle.
func (h *rsHarness) verifyIndexes(label string, db *storage.DB, o rsOracle) {
	h.t.Helper()
	bySource := map[string][]*rsRow{}
	for _, id := range o.sortedIDs() {
		bySource[o[id].source] = append(bySource[o[id].source], o[id])
	}
	ask := func(what, sql string, want []int) {
		h.t.Helper()
		rs := h.must(db, h.tenant, sql)
		got := rsIDs(rs)
		sort.Ints(got)
		if want == nil {
			want = []int{}
		}
		if !reflect.DeepEqual(got, want) {
			h.fatalf("%s: %s: %s returned %v, the oracle has %v", label, what, sql, rsHead(got), rsHead(want))
		}
	}
	idsOf := func(rows []*rsRow, keep func(*rsRow) bool) []int {
		out := []int{}
		for _, r := range rows {
			if keep == nil || keep(r) {
				out = append(out, r.id)
			}
		}
		sort.Ints(out)
		return out
	}
	for s := 0; s <= rsSources; s++ {
		name := rsSourceName(s)
		rows := bySource[name]
		ask("index on source_id", fmt.Sprintf(`SELECT id FROM chunks WHERE source_id = '%s'`, name), idsOf(rows, nil))
		ask("scan on source_id", fmt.Sprintf(`SELECT id FROM chunks WHERE UPPER(source_id) = '%s'`, strings.ToUpper(name)), idsOf(rows, nil))
		ask("index on both columns", fmt.Sprintf(`SELECT id FROM chunks WHERE source_id = '%s' AND chunk_idx = 2`, name), idsOf(rows, func(r *rsRow) bool { return r.idx == 2 }))
		ask("scan on both columns", fmt.Sprintf(`SELECT id FROM chunks WHERE UPPER(source_id) = '%s' AND chunk_idx + 0 = 2`, strings.ToUpper(name)), idsOf(rows, func(r *rsRow) bool { return r.idx == 2 }))
		ask("index range on the second column", fmt.Sprintf(`SELECT id FROM chunks WHERE source_id = '%s' AND chunk_idx >= 3`, name), idsOf(rows, func(r *rsRow) bool { return r.idx >= 3 }))
	}
	for _, group := range [][]int{{1, 2, 3}, {10, 11, 12, 13, 14, 15}, {0, 89, 90}} {
		names := make([]string, len(group))
		pick := map[string]bool{}
		for i, s := range group {
			names[i] = rsSourceName(s)
			pick[names[i]] = true
		}
		var rows []*rsRow
		for _, id := range o.sortedIDs() {
			if pick[o[id].source] {
				rows = append(rows, o[id])
			}
		}
		ask("IN on the index", fmt.Sprintf(`SELECT id FROM chunks WHERE source_id IN (%s)`, rsInList(names)), idsOf(rows, nil))
	}
	if h.variant == 1 {
		loads := map[string][]*rsRow{}
		for _, id := range o.sortedIDs() {
			loads[o[id].load] = append(loads[o[id].load], o[id])
		}
		n := 0
		for load, rows := range loads {
			if n++; n > 40 {
				break
			}
			ask("single column index", fmt.Sprintf(`SELECT id FROM chunks WHERE load_id = '%s'`, load), idsOf(rows, nil))
			ask("scan on load_id", fmt.Sprintf(`SELECT id FROM chunks WHERE LOWER(load_id) = '%s'`, strings.ToLower(load)), idsOf(rows, nil))
		}
		ask("missing load", `SELECT id FROM chunks WHERE load_id = 'L-none'`, nil)
		ids := o.sortedIDs()
		for i := 0; i < len(ids); i += max(1, len(ids)/40) {
			ask("unique index", fmt.Sprintf(`SELECT id FROM chunks WHERE id = %d`, ids[i]), []int{ids[i]})
		}
		ask("unique index, missing", fmt.Sprintf(`SELECT id FROM chunks WHERE id = %d`, h.nextID+5000), nil)
	}
}

// verifySearches compares the searches of db with those of fresh in-memory
// databases built from the oracle: bit for bit against one built in db's own
// physical row order (rows, ranks and scores, ties included), and id by id
// against one built in id order (the oracle's own order, where ties may rank
// differently but every document has the one score its content and the corpus
// give it).
func (h *rsHarness) verifySearches(label string, db *storage.DB, o rsOracle) {
	h.t.Helper()
	physical := rsIDs(h.must(db, h.tenant, `SELECT id FROM chunks`))
	qs := h.querySet()

	fa, ta := h.freshDB(physical, o)
	defer h.dropFresh(fa, ta)
	for _, q := range qs {
		got := h.must(db, h.tenant, q.sql)
		want := h.must(fa, ta, q.sql)
		if !reflect.DeepEqual(got.Rows, want.Rows) {
			h.fatalf("%s: live and fresh answers (physical order) differ for\n  %s\n%s", label, q.sql, rsFirstDiff(got.Rows, want.Rows))
		}
	}

	fb, tb := h.freshDB(o.sortedIDs(), o)
	defer h.dropFresh(fb, tb)
	scoreOf := map[string]string{"fts": "_fts_score", "vec": "_vec_distance"}
	for _, q := range qs {
		col, ok := scoreOf[q.kind]
		if !ok || !q.full {
			continue
		}
		got := h.must(db, h.tenant, q.sql)
		want := h.must(fb, tb, q.sql)
		if len(got.Rows) != len(want.Rows) {
			h.fatalf("%s: live and fresh answers (id order) differ in length for\n  %s\nlive %d rows, fresh %d rows", label, q.sql, len(got.Rows), len(want.Rows))
		}
		ws := make(map[int]float64, len(want.Rows))
		for _, r := range want.Rows {
			ws[rsInt(r["id"])] = rsFloat(r[col])
		}
		for _, r := range got.Rows {
			id := rsInt(r["id"])
			w, ok := ws[id]
			if !ok || math.Abs(rsFloat(r[col])-w) > 1e-12*math.Max(1, math.Abs(w)) {
				h.fatalf("%s: live and fresh answers (id order) differ for\n  %s\nid %d: live %s %v, fresh %v (present: %v)", label, q.sql, id, col, r[col], w, ok)
			}
		}
	}
}

// verifyRecall measures the recall of the HNSW graph of db against the exact top
// 10 computed from the oracle, and the distances it reports.
func (h *rsHarness) verifyRecall(label string, db *storage.DB, o rsOracle) float64 {
	h.t.Helper()
	r := rand.New(rand.NewSource(h.seed ^ 0xa11))
	ids := o.sortedIDs()
	total, hits := 0, 0
	for i := 0; i < 12; i++ {
		q := make([]float64, rsDims)
		for d := range q {
			q[d] = float64(r.Intn(2001)-1000) / 1000
		}
		type scored struct {
			id int
			d  float64
		}
		all := make([]scored, len(ids))
		for j, id := range ids {
			all[j] = scored{id, rsCosineDistance(o[id].vec, q)}
		}
		sort.SliceStable(all, func(a, b int) bool { return all[a].d < all[b].d })
		truth := map[int]bool{}
		for _, s := range all[:min(10, len(all))] {
			truth[s.id] = true
		}
		rs := h.must(db, h.tenant, fmt.Sprintf(`SELECT id, _vec_distance FROM VEC_SEARCH('chunks', 'embedding', '%s', 10, 'cosine', 'hnsw')`, retrievalVecLiteral(q)))
		for _, row := range rs.Rows {
			id := rsInt(row["id"])
			w := o[id]
			if w == nil {
				h.fatalf("%s: HNSW returned row %d that does not exist", label, id)
			}
			if d := rsCosineDistance(w.vec, q); math.Abs(rsFloat(row["_vec_distance"])-d) > 1e-9 {
				h.fatalf("%s: HNSW reports distance %v for row %d, its vector gives %v", label, row["_vec_distance"], id, d)
			}
			if truth[id] {
				hits++
			}
		}
		total += len(truth)
	}
	recall := float64(hits) / float64(total)
	if recall < rsMinRecall {
		h.fatalf("%s: HNSW recall@10 is %.3f, below %.2f", label, recall, rsMinRecall)
	}
	return recall
}

func (h *rsHarness) verify(label string, db *storage.DB, o rsOracle, s rsSide) {
	h.t.Helper()
	h.verifyTable(label, db, o)
	h.verifySide(label, db, s)
	h.verifyIndexes(label, db, o)
	h.verifySearches(label, db, o)
	recall := h.verifyRecall(label, db, o)
	h.t.Logf("%s: %s: %d rows verified, HNSW recall@10 %.3f", h.tag(), label, len(o), recall)
}

// ---------------------------------------------------------------------------
// one run: workload, verification, reopen, continued workload, crash copies

func (h *rsHarness) open(path string) *storage.DB {
	h.t.Helper()
	db, err := storage.OpenDB(h.cfg.storage(path))
	if err != nil {
		h.fatalf("open %s: %v", path, err)
	}
	return db
}

func (h *rsHarness) run(iters int) {
	h.t.Helper()
	h.dir = filepath.Join(h.t.TempDir(), "db")
	h.db = h.open(h.dir)
	h.resetHistory()
	h.chkDir = filepath.Join(h.t.TempDir(), "chk")
	closed := false
	h.t.Cleanup(func() {
		if !closed && h.db != nil {
			// A panic inside the storage code (the twin verification) can leave one of
			// its locks held, and Close would then wait for ever instead of letting the
			// panic be reported.
			done := make(chan struct{})
			go func() { defer close(done); _ = h.db.Close() }()
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				h.t.Logf("%s: Close did not return, a lock is still held", h.tag())
			}
		}
		purgeFTSCachesFor(h.tenant, "chunks")
		purgeVectorCachesFor(h.tenant, "chunks")
	})
	h.createSchema()
	h.oracle = rsOracle{}
	h.side = rsSide{}
	tg := &rsTarget{db: h.db, o: h.oracle, s: h.side}
	for s := 0; s < rsSources; s++ {
		rows, _ := h.newRows(s, 4+h.rng.Intn(5))
		h.insertRows(tg, rows)
	}
	for g := 0; g < 30; g++ {
		h.sideInsert(tg, h.sideRows(fmt.Sprintf("g%d", g), 12))
	}
	h.clock = 100

	h.phase(iters,
		map[int]bool{iters / 3: true, 2 * iters / 3: true},
		map[int]bool{iters * 2 / 5: true, iters * 3 / 4: true})
	h.verify("live", h.db, h.oracle, h.side)

	// Sync, close, reopen: the persisted images and files describe the oracle.
	if err := h.db.Sync(); err != nil {
		h.fatalf("Sync: %v", err)
	}
	if err := h.db.Close(); err != nil {
		h.fatalf("Close: %v", err)
	}
	closed = true
	h.db = h.open(h.dir)
	h.resetHistory()
	closed = false
	h.stats.reopens++
	// A pre-existing defect of the base version (v0.75.0), found by this test and
	// unrelated to the retrieval work: in ModeHybrid a table that was loaded from
	// the files lives in the buffer pool, not in the tenant map that
	// SnapshotForTxShared copies, so a transaction writes to the live table directly
	// and its ROLLBACK leaves the changes in place (a table created in the same
	// process is fine). Hybrid runs therefore use no explicit transactions after the
	// first reopen.
	h.noTx = h.cfg.mode == storage.ModeHybrid
	if tbl, err := h.db.Get(h.tenant, "chunks"); err != nil {
		h.fatalf("reopened: %v", err)
	} else if h.cfg.skip && (len(tbl.FTSIndexes) != 0 || len(tbl.VectorIndexes) != 0) {
		h.fatalf("SkipSearchIndexPersistence: the reopened table carries search index images")
	} else if !h.cfg.skip && len(tbl.FTSIndexes) == 0 {
		h.fatalf("the reopened table carries no FTS image although it was searched before the Sync")
	}
	h.verify("reopened", h.db, h.oracle, h.side)

	// The workload goes on after the reopen (the change log starts empty, the
	// images come from the file), then once more through Sync, Close and reopen.
	h.phase(iters/4, map[int]bool{iters / 8: true}, nil)
	h.verify("reopened and continued", h.db, h.oracle, h.side)
	if err := h.db.Sync(); err != nil {
		h.fatalf("Sync: %v", err)
	}
	if err := h.db.Close(); err != nil {
		h.fatalf("Close: %v", err)
	}
	closed = true
	h.db = h.open(h.dir)
	closed = false
	h.stats.reopens++
	h.verifyTable("reopened twice", h.db, h.oracle)
	h.verifySearches("reopened twice", h.db, h.oracle)
	h.verifySide("reopened twice", h.db, h.side)

	// Crash copies: files as a Sync left them, opened without any Close before.
	for _, c := range h.crashes {
		db, err := storage.OpenDB(h.cfg.storage(c.dir))
		if err != nil {
			h.fatalf("%s: open the copy: %v", c.label, err)
		}
		saved := h.db
		h.db = db
		h.verify(c.label, db, c.oracle, c.side)
		h.db = saved
		_ = db.Close()
	}
}

func rsConfigs() []rsConfig {
	return []rsConfig{
		{name: "disk-default", mode: storage.ModeDisk},
		{name: "disk-skip", mode: storage.ModeDisk, skip: true},
		{name: "disk-rowblocks-skip", mode: storage.ModeDisk, skip: true, blocks: true},
		{name: "hybrid-default", mode: storage.ModeHybrid},
		{name: "hybrid-skip", mode: storage.ModeHybrid, skip: true},
		{name: "hybrid-rowblocks-skip", mode: storage.ModeHybrid, skip: true, blocks: true},
	}
}

// TestRetrievalMaintenanceStress is the end-to-end test described at the top of
// this file.
func TestRetrievalMaintenanceStress(t *testing.T) {
	seeds, iters := []int64{1, 2}, 220
	if testing.Short() {
		seeds, iters = []int64{1}, 90
	}
	was := storage.SetTwinVerification(true)
	t.Cleanup(func() { storage.SetTwinVerification(was) })
	columnFollows, hnswFollows := vecColumnFollowCount.Load(), vecHNSWFollowCount.Load()
	rowTwins0, indexTwins0 := storage.TwinTakes()
	var total rsStats
	for _, cfg := range rsConfigs() {
		for _, seed := range seeds {
			if seed > 1 && !strings.HasSuffix(cfg.name, "-default") {
				continue // two seeds for the default options, one for the others
			}
			for variant := 0; variant < 2; variant++ {
				name := fmt.Sprintf("%s/seed=%d/variant=%d", cfg.name, seed, variant)
				t.Run(name, func(t *testing.T) {
					h := &rsHarness{
						t: t, seed: seed*131 + int64(variant), variant: variant, cfg: cfg,
						tenant: fmt.Sprintf("rs-%s-%d-%d", cfg.name, seed, variant),
					}
					h.rng = rand.New(rand.NewSource(h.seed))
					start := time.Now()
					h.run(iters)
					s := &h.stats
					t.Logf("%s: %v: %d statements (%d replaces, %d deletes of %d rows, %d touches of %d rows, %d content and %d embedding updates, %d failed, %d no-ops), %d commits, %d rollbacks, %d savepoints (%d rolled back to), %d cap phases, %d oracle checks, %d crash copies, %d reopens; background: %d syncs, %d searches, %d copies of the files checked (%d unmatched)",
						h.tag(), time.Since(start).Round(time.Millisecond), s.statements, s.replaces, s.deletes, s.deletedRows, s.touches, s.touchedRows,
						s.contentUpdates, s.embeddingUpdates, s.failed, s.noops, s.txCommits, s.txRollbacks, s.savepoints, s.savepointRollbacks,
						s.capPhases, s.checks, s.crashCopies, s.reopens, s.syncs.Load(), s.reads.Load(), s.copyChecked.Load(), s.copyUnmatched.Load())
					total.deletes += s.deletes
					total.deletedRows += s.deletedRows
					total.emptyDeletes += s.emptyDeletes
					total.touches += s.touches
					total.failed += s.failed
					total.txCommits += s.txCommits
					total.txRollbacks += s.txRollbacks
					total.savepointRollbacks += s.savepointRollbacks
					total.capPhases += s.capPhases
					total.syncs.Add(s.syncs.Load())
					total.reads.Add(s.reads.Load())
					total.copyChecked.Add(s.copyChecked.Load())
					total.copyUnmatched.Add(s.copyUnmatched.Load())
				})
			}
		}
	}
	if t.Failed() {
		return
	}
	columnFollows = vecColumnFollowCount.Load() - columnFollows
	hnswFollows = vecHNSWFollowCount.Load() - hnswFollows
	rowTwins, indexTwins := storage.TwinTakes()
	rowTwins, indexTwins = rowTwins-rowTwins0, indexTwins-indexTwins0
	t.Logf("transactions took %d row-header arrays and %d index skip lists from twins instead of copying them", rowTwins, indexTwins)
	t.Logf("in all: %d deletes (%d empty) of %d rows, %d touches, %d failed statements, %d commits, %d rollbacks, %d savepoint rollbacks, %d cap phases, %d background syncs, %d background searches, %d copies of the files checked (%d unmatched); the flat vector cache followed %d removals, the HNSW graph %d",
		total.deletes, total.emptyDeletes, total.deletedRows, total.touches, total.failed, total.txCommits, total.txRollbacks, total.savepointRollbacks, total.capPhases, total.syncs.Load(), total.reads.Load(), total.copyChecked.Load(), total.copyUnmatched.Load(), columnFollows, hnswFollows)
	// The workload must have exercised what the test claims to.
	if total.deletes < 500 || total.touches < 100 || total.failed < 50 || total.txCommits < 5 || total.txRollbacks < 5 ||
		total.savepointRollbacks < 3 || total.capPhases < 4 || total.syncs.Load() < 50 || total.reads.Load() < 500 || total.copyChecked.Load() < 50 ||
		columnFollows == 0 || hnswFollows == 0 || rowTwins == 0 || indexTwins == 0 {
		t.Fatalf("the workload covered too little")
	}
}

// twinBurst runs transactions on the side table one after the other, with an
// autocommit statement now and then in between. A committed transaction parks the
// row headers and the index skip lists it replaced as twins, and the next
// transaction that writes the table takes them over after replaying what changed;
// a DELETE in between renumbers the live index in place, which must make the
// parked twin unusable.
func (h *rsHarness) twinBurst() {
	for i := 0; i < 10; i++ {
		h.runTx(true)
		if i%4 == 3 {
			h.opSide(&rsTarget{db: h.db, o: h.oracle, s: h.side}, h.rng.Intn(3) != 0)
		}
	}
}
