package engine

import (
	"context"
	"fmt"
	"math/rand"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// Second differential test of the incremental maintenance, on a 72-column table:
// the indexed text columns sit at positions 62, 63 and 70 (the column mask gives
// every column from 63 on one shared bit), two of them are nullable, there are
// full-text column sets of one and of two columns, and the statement mix has
// upserts, NULL rewrites, a late ALTER TABLE ADD COLUMN, index DDL and statements
// that fail midway. The live database answers from structures it maintained
// incrementally; a cold database loaded with the same rows must answer
// identically.

const (
	wideColBody = 63
	wideColVec  = 71
)

func wideCreateSQL() string {
	var b strings.Builder
	b.WriteString("CREATE TABLE wide (id INT PRIMARY KEY")
	for i := 1; i <= 61; i++ {
		fmt.Fprintf(&b, ", f%d INT", i)
	}
	b.WriteString(", title TEXT, body TEXT, pad INT")
	for i := 65; i <= 69; i++ {
		fmt.Fprintf(&b, ", e%d INT", i)
	}
	b.WriteString(", memo TEXT, embedding VECTOR)")
	return b.String()
}

type wideHarness struct {
	t       *testing.T
	rng     *rand.Rand
	tenant  string
	db      *storage.DB
	dir     string
	nextID  int
	label   string
	altered bool
}

func (h *wideHarness) exec(db *storage.DB, tenant, sql string) (*ResultSet, error) {
	return Execute(context.Background(), db, tenant, mustParse(sql))
}

func (h *wideHarness) mustExec(sql string) *ResultSet {
	h.t.Helper()
	rs, err := h.exec(h.db, h.tenant, sql)
	if err != nil {
		h.t.Fatalf("%s: %s: %v", h.label, sql, err)
	}
	return rs
}

func (h *wideHarness) text() string {
	n := 2 + h.rng.Intn(7)
	words := make([]string, n)
	for i := range words {
		switch r := h.rng.Intn(10); {
		case r < 7:
			words[i] = fmt.Sprintf("w%d", h.rng.Intn(20))
		case r < 9:
			words[i] = fmt.Sprintf("tag%d", h.rng.Intn(5))
		default:
			words[i] = fmt.Sprintf("rare%d", h.rng.Intn(300))
		}
	}
	return strings.Join(words, " ")
}

// nullable returns a SQL literal: NULL one time in six, else a text.
func (h *wideHarness) nullable() string {
	if h.rng.Intn(6) == 0 {
		return "NULL"
	}
	return "'" + h.text() + "'"
}

func (h *wideHarness) vec() string {
	v := make([]float64, 4)
	for i := range v {
		v[i] = float64(h.rng.Intn(2001)-1000) / 1000
	}
	return "'" + retrievalVecLiteral(v) + "'"
}

func (h *wideHarness) open() {
	h.t.Helper()
	if h.dir == "" {
		h.db = storage.NewDB()
		return
	}
	db, err := storage.OpenDB(storage.StorageConfig{Mode: storage.ModeDisk, Path: h.dir})
	if err != nil {
		h.t.Fatal(err)
	}
	h.db = db
}

func (h *wideHarness) insert(n int) {
	for i := 0; i < n; i++ {
		h.mustExec(fmt.Sprintf(`INSERT INTO wide (id, f1, f61, title, body, pad, e65, memo, embedding) VALUES (%d, %d, %d, '%s', %s, %d, %d, %s, %s)`,
			h.nextID, h.rng.Intn(50), h.rng.Intn(50), h.text(), h.nullable(), h.rng.Intn(50), h.rng.Intn(50), h.nullable(), h.vec()))
		h.nextID++
	}
}

func (h *wideHarness) step() string {
	m := 2 + h.rng.Intn(8)
	r := h.rng.Intn(m)
	where := fmt.Sprintf("WHERE id %% %d = %d", m, r)
	switch op := h.rng.Intn(18); op {
	case 0, 1:
		n := 1 + h.rng.Intn(5)
		h.insert(n)
		return fmt.Sprintf("insert %d", n)
	case 2: // a column far from the indexed ones
		h.mustExec(fmt.Sprintf(`UPDATE wide SET f1 = %d %s`, h.rng.Intn(1000), where))
		return "touch f1"
	case 3: // the bookkeeping column that shares bit 63 with body and memo
		h.mustExec(fmt.Sprintf(`UPDATE wide SET pad = %d %s`, 1000+h.rng.Intn(1000), where))
		return "touch pad"
	case 4:
		h.mustExec(fmt.Sprintf(`UPDATE wide SET pad = %d, f61 = %d, e65 = %d`, h.rng.Intn(100), h.rng.Intn(100), h.rng.Intn(100)))
		return "touch all"
	case 5:
		h.mustExec(fmt.Sprintf(`UPDATE wide SET title = '%s' %s`, h.text(), where))
		return "title"
	case 6:
		h.mustExec(fmt.Sprintf(`UPDATE wide SET body = %s %s`, h.nullable(), where))
		return "body"
	case 7:
		h.mustExec(fmt.Sprintf(`UPDATE wide SET memo = %s %s`, h.nullable(), where))
		return "memo"
	case 8:
		h.mustExec(fmt.Sprintf(`UPDATE wide SET body = NULL, memo = NULL %s`, where))
		return "null rewrite"
	case 9:
		h.mustExec(fmt.Sprintf(`UPDATE wide SET embedding = %s %s`, h.vec(), where))
		return "embedding"
	case 10:
		h.mustExec(fmt.Sprintf(`UPDATE wide SET title = '%s', body = %s, pad = %d, embedding = %s %s`, h.text(), h.nullable(), h.rng.Intn(9), h.vec(), where))
		return "multi"
	case 11: // upsert of an existing row
		id := h.rng.Intn(h.nextID)
		h.mustExec(fmt.Sprintf(`INSERT INTO wide (id, title, body) VALUES (%d, '%s', %s) ON CONFLICT (id) DO UPDATE SET body = %s, pad = 7`, id, h.text(), h.nullable(), h.nullable()))
		return "upsert"
	case 12:
		h.mustExec(`UPDATE wide SET title = title, body = body, f1 = f1`)
		return "update identical"
	case 13:
		h.mustExec(fmt.Sprintf(`DELETE FROM wide %s AND f1 %% 3 = 0`, where))
		return "delete"
	case 14:
		h.mustExec(`DELETE FROM wide WHERE id < 0`)
		return "delete nothing"
	case 15:
		if !h.altered && h.rng.Intn(3) == 0 {
			h.altered = true
			h.mustExec(`ALTER TABLE wide ADD COLUMN late INT`)
			return "alter add column"
		}
		h.mustExec(fmt.Sprintf(`UPDATE wide SET pad = %d %s`, h.rng.Intn(10), where))
		return "touch pad (b)"
	case 16:
		h.mustExec(`CREATE INDEX IF NOT EXISTS wide_f1 ON wide (f1)`)
		return "create index"
	default:
		// Fails after rewriting some rows: nothing may stick. The divisor is zero
		// for one row that exists (the table has holes after DELETEs).
		ids := h.mustExec(`SELECT id FROM wide`).Rows
		if len(ids) < 3 {
			return "failed statement (skipped)"
		}
		victim, _ := toInt(ids[len(ids)*2/3]["id"])
		sql := fmt.Sprintf(`UPDATE wide SET title = 'ghost phantom', body = 'ghost', embedding = %s, f1 = 1 / (id - %d)`, h.vec(), victim)
		if _, err := h.exec(h.db, h.tenant, sql); err == nil {
			h.t.Fatalf("%s: the failing statement succeeded", h.label)
		}
		return "failed statement"
	}
}

func (h *wideHarness) queries() []string {
	var qs []string
	for _, cols := range []string{"'title'", "'body'", "'memo'", "'title', 'body'"} {
		for _, term := range []string{
			fmt.Sprintf("w%d", h.rng.Intn(20)),
			fmt.Sprintf("w%d OR tag%d", h.rng.Intn(20), h.rng.Intn(5)),
			fmt.Sprintf("rare%d OR w%d", h.rng.Intn(300), h.rng.Intn(20)),
			"ghost",
		} {
			qs = append(qs, fmt.Sprintf(`SELECT id, _fts_rank, _fts_score FROM FTS_SEARCH('wide', '%s', 8, %s)`, term, cols))
		}
	}
	q := strings.Trim(h.vec(), "'")
	qs = append(qs,
		fmt.Sprintf(`SELECT id, _vec_distance FROM VEC_SEARCH('wide', 'embedding', '%s', 6, 'cosine')`, q),
		fmt.Sprintf(`SELECT id, _vec_distance FROM VEC_SEARCH('wide', 'embedding', '%s', 5, 'l2')`, q),
	)
	return qs
}

var wideTracked = []string{"id", "f1", "f61", "title", "body", "pad", "e65", "memo", "embedding"}

func wideLiteral(v any) string {
	switch x := v.(type) {
	case nil:
		return "NULL"
	case string:
		return "'" + x + "'"
	case []float64:
		return "'" + retrievalVecLiteral(x) + "'"
	default:
		return fmt.Sprint(x)
	}
}

func (h *wideHarness) compare(step int, what string) {
	h.t.Helper()
	rs := h.mustExec(`SELECT ` + strings.Join(wideTracked, ", ") + ` FROM wide`)
	coldTenant := fmt.Sprintf("widecold-%s-%d", h.tenant, step)
	cold := storage.NewDB()
	run := func(db *storage.DB, tenant, sql string) *ResultSet {
		rs, err := h.exec(db, tenant, sql)
		if err != nil {
			h.t.Fatalf("%s step %d (%s): %s: %v", h.label, step, what, sql, err)
		}
		return rs
	}
	run(cold, coldTenant, wideCreateSQL())
	for _, row := range rs.Rows {
		vals := make([]string, len(wideTracked))
		for i, col := range wideTracked {
			vals[i] = wideLiteral(row[col])
		}
		run(cold, coldTenant, fmt.Sprintf(`INSERT INTO wide (%s) VALUES (%s)`, strings.Join(wideTracked, ", "), strings.Join(vals, ", ")))
	}
	for _, q := range h.queries() {
		got, want := run(h.db, h.tenant, q), run(cold, coldTenant, q)
		if !reflect.DeepEqual(got.Rows, want.Rows) {
			h.t.Fatalf("%s step %d (%s): live and cold answers differ for\n  %s\nlive: %v\ncold: %v", h.label, step, what, q, got.Rows, want.Rows)
		}
	}
	purgeFTSCachesFor(coldTenant, "wide")
	purgeVectorCachesFor(coldTenant, "wide")
}

func (h *wideHarness) warm() {
	switch h.rng.Intn(5) {
	case 0:
		h.mustExec(`SELECT * FROM FTS_WARM('wide', 'title')`)
	case 1:
		h.mustExec(`SELECT * FROM FTS_WARM('wide', 'body')`)
	case 2:
		h.mustExec(`SELECT * FROM FTS_WARM('wide', 'memo')`)
	case 3:
		h.mustExec(`SELECT * FROM VEC_WARM('wide', 'embedding')`)
	}
}

func runWideDifferential(t *testing.T, seed int64, steps int, reopen bool) {
	h := &wideHarness{t: t, rng: rand.New(rand.NewSource(seed)), tenant: fmt.Sprintf("widelive-%d-%t", seed, reopen)}
	if reopen {
		h.dir = filepath.Join(t.TempDir(), "db")
	}
	h.open()
	t.Cleanup(func() {
		if h.db != nil {
			_ = h.db.Close()
		}
		purgeFTSCachesFor(h.tenant, "wide")
		purgeVectorCachesFor(h.tenant, "wide")
	})
	h.mustExec(wideCreateSQL())
	h.insert(100)
	h.warm()
	h.compare(0, "initial")
	var log []string
	for step := 1; step <= steps; step++ {
		what := h.step()
		log = append(log, what)
		h.label = fmt.Sprintf("seed %d reopen=%t [%s]", seed, reopen, strings.Join(log[max(0, len(log)-6):], "; "))
		if h.rng.Intn(3) == 0 {
			h.warm()
		}
		if reopen && step%13 == 0 {
			if err := h.db.Sync(); err != nil {
				t.Fatal(err)
			}
			purgeFTSCachesFor(h.tenant, "wide")
			purgeVectorCachesFor(h.tenant, "wide")
			if err := h.db.Close(); err != nil {
				t.Fatal(err)
			}
			h.open()
			what += " (reopened)"
		}
		if h.rng.Intn(4) == 0 || step == steps {
			h.compare(step, what)
		}
	}
}

func TestRetrievalMaintenanceWideTableMatchesColdBuild(t *testing.T) {
	for _, seed := range []int64{21, 22, 23, 24, 25} {
		t.Run(fmt.Sprintf("memory/seed%d", seed), func(t *testing.T) {
			runWideDifferential(t, seed, 50, false)
		})
	}
	t.Run("disk/seed26", func(t *testing.T) { runWideDifferential(t, 26, 50, true) })
}

// More deltas than the log holds between two searches: the folded summary still
// proves that the indexed columns did not change, an indexed rewrite buried in
// the folded range is not lost, and the structures are kept (not rebuilt) when
// nothing relevant happened.
func TestRetrievalMaintenanceSurvivesFoldedUpdateLog(t *testing.T) {
	h := &wideHarness{t: t, rng: rand.New(rand.NewSource(77)), tenant: "widefold", label: "fold"}
	h.open()
	t.Cleanup(func() {
		purgeFTSCachesFor(h.tenant, "wide")
		purgeVectorCachesFor(h.tenant, "wide")
	})
	h.mustExec(wideCreateSQL())
	h.insert(300)
	table, err := h.db.Get(h.tenant, "wide")
	if err != nil {
		t.Fatal(err)
	}
	h.mustExec(`SELECT * FROM FTS_SEARCH('wide', 'w3 OR tag1', 20, 'body')`)
	h.mustExec(`SELECT * FROM VEC_SEARCH('wide', 'embedding', '[0.1, 0.2, 0.3, 0.4]', 6, 'cosine')`)
	bodyKey := ftsColsCacheKey([]int{wideColBody})
	index := table.FTSIndexes[bodyKey]
	if index == nil {
		t.Fatal("no FTS index after the first search")
	}
	cache := getVecColumnCache(h.tenant, table, wideColVec, true)

	// 90k updated rows, far past the 65536-delta log, none of them indexed. (Columns
	// from 63 on share one mask bit, so f1 and f61 are the unrelated ones here.)
	for i := 0; i < 300; i++ {
		h.mustExec(fmt.Sprintf(`UPDATE wide SET f1 = %d, f61 = %d`, i, i))
	}
	h.compare(1, "after 300 whole-table touches")
	if table.FTSIndexes[bodyKey] != index {
		t.Fatal("the full-text index was rebuilt although no indexed column changed")
	}
	if again := getVecColumnCache(h.tenant, table, wideColVec, true); &again.segments[0].data[0] != &cache.segments[0].data[0] {
		t.Fatal("the vector column cache was rebuilt although the vector column did not change")
	}

	// An indexed rewrite followed by enough touches to fold it away is still seen.
	h.mustExec(`UPDATE wide SET body = 'w3 tag1 buried rewrite' WHERE id = 5`)
	for i := 0; i < 300; i++ {
		h.mustExec(fmt.Sprintf(`UPDATE wide SET f61 = %d`, i))
	}
	if rs := h.mustExec(`SELECT * FROM FTS_SEARCH('wide', 'buried', 5, 'body')`); len(rs.Rows) != 1 {
		t.Fatalf("a rewrite buried under folded touches was lost: %d rows for 'buried'", len(rs.Rows))
	}
	h.compare(2, "after a buried rewrite")
}
