package engine

import (
	"context"
	"fmt"
	"math/rand"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// Differential test of the incremental maintenance of the full-text index, the
// vector column cache and the HNSW graph. A live database receives a seeded
// random sequence of INSERT / UPDATE / DELETE statements (touching a bookkeeping
// column, rewriting text, rewriting embeddings, doing nothing, failing midway)
// and answers searches from caches it maintained incrementally; at random
// points a fresh database loaded with the very same rows answers the same
// searches from structures built cold, and every result row, rank and score must
// be identical bit for bit.

const retrievalDiffDims = 6

type retrievalDiffRow struct {
	id, loaded int
	content    string
	vec        []float64
}

type retrievalDiffHarness struct {
	t      *testing.T
	rng    *rand.Rand
	tenant string
	db     *storage.DB
	dir    string // "" for an in-memory database
	model  map[int]*retrievalDiffRow
	nextID int
	label  string
	// annMoved: a vector was rewritten since the HNSW graph was last rebuilt.
	annMoved bool
	// followSeen is vecHNSWFollowCount as of the last delete or HNSW search the
	// harness looked at: a removal the graph followed (instead of a rebuild) moves it.
	followSeen int64
	// structural adds the statements that rebuild or replace the table (see
	// structuralStep); off for the original seeds so that their runs stay as they were.
	structural bool
}

func retrievalVecLiteral(v []float64) string {
	parts := make([]string, len(v))
	for i, x := range v {
		parts[i] = strconv.FormatFloat(x, 'g', -1, 64)
	}
	return "[" + strings.Join(parts, ", ") + "]"
}

func (h *retrievalDiffHarness) exec(db *storage.DB, tenant, sql string) (*ResultSet, error) {
	return Execute(context.Background(), db, tenant, mustParse(sql))
}

func (h *retrievalDiffHarness) mustExec(sql string) *ResultSet {
	h.t.Helper()
	rs, err := h.exec(h.db, h.tenant, sql)
	if err != nil {
		h.t.Fatalf("%s: %s: %v", h.label, sql, err)
	}
	return rs
}

func (h *retrievalDiffHarness) randText() string {
	if h.rng.Intn(14) == 0 {
		return "" // a document without tokens
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
			words[i] = fmt.Sprintf("rare%d", h.rng.Intn(400)) // new terms keep appearing
		}
	}
	return strings.Join(words, " ")
}

func (h *retrievalDiffHarness) randVec() []float64 {
	v := make([]float64, retrievalDiffDims)
	for i := range v {
		v[i] = float64(h.rng.Intn(2001)-1000) / 1000
	}
	return v
}

func (h *retrievalDiffHarness) open() {
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

func (h *retrievalDiffHarness) insert(n int) {
	for i := 0; i < n; i++ {
		row := &retrievalDiffRow{id: h.nextID, loaded: h.rng.Intn(100), content: h.randText(), vec: h.randVec()}
		h.nextID++
		h.model[row.id] = row
		h.mustExec(fmt.Sprintf(`INSERT INTO docs VALUES (%d, %d, '%s', '%s')`, row.id, row.loaded, row.content, retrievalVecLiteral(row.vec)))
	}
}

// matching returns the model rows a "id % m = r" predicate selects.
func (h *retrievalDiffHarness) matching(m, r int) []*retrievalDiffRow {
	var out []*retrievalDiffRow
	for _, row := range h.model {
		if row.id%m == r {
			out = append(out, row)
		}
	}
	return out
}

// step applies one random statement to the live database and to the model.
func (h *retrievalDiffHarness) step() string {
	if h.structural && h.rng.Intn(9) == 0 {
		return h.structuralStep()
	}
	m, r := 2+h.rng.Intn(9), 0
	r = h.rng.Intn(m)
	switch op := h.rng.Intn(16); op {
	case 0, 1:
		n := 1 + h.rng.Intn(6)
		h.insert(n)
		return fmt.Sprintf("insert %d", n)
	case 2, 3, 4: // the importer's shape: only the bookkeeping column
		stamp := 1000 + h.rng.Intn(1000)
		for _, row := range h.matching(m, r) {
			row.loaded = stamp
		}
		h.mustExec(fmt.Sprintf(`UPDATE docs SET loaded_at = %d WHERE id %% %d = %d`, stamp, m, r))
		return fmt.Sprintf("touch %%%d=%d", m, r)
	case 5:
		stamp := 2000 + h.rng.Intn(1000)
		for _, row := range h.model {
			row.loaded = stamp
		}
		h.mustExec(fmt.Sprintf(`UPDATE docs SET loaded_at = %d`, stamp))
		return "touch all"
	case 6, 7:
		text := h.randText()
		for _, row := range h.matching(m, r) {
			row.content = text
		}
		h.mustExec(fmt.Sprintf(`UPDATE docs SET content = '%s' WHERE id %% %d = %d`, text, m, r))
		return fmt.Sprintf("content %%%d=%d", m, r)
	case 8, 9:
		vec := h.randVec()
		for _, row := range h.matching(m, r) {
			row.vec = vec
		}
		h.mustExec(fmt.Sprintf(`UPDATE docs SET embedding = '%s' WHERE id %% %d = %d`, retrievalVecLiteral(vec), m, r))
		h.annMoved = true
		return fmt.Sprintf("embedding %%%d=%d", m, r)
	case 10:
		text, vec, stamp := h.randText(), h.randVec(), 3000+h.rng.Intn(1000)
		for _, row := range h.matching(m, r) {
			row.content, row.vec, row.loaded = text, vec, stamp
		}
		h.mustExec(fmt.Sprintf(`UPDATE docs SET loaded_at = %d, content = '%s', embedding = '%s' WHERE id %% %d = %d`, stamp, text, retrievalVecLiteral(vec), m, r))
		h.annMoved = true
		return fmt.Sprintf("multi %%%d=%d", m, r)
	case 11:
		h.mustExec(`UPDATE docs SET loaded_at = 5 WHERE id < 0`)
		return "update nothing"
	case 12:
		h.mustExec(`UPDATE docs SET loaded_at = loaded_at, content = content`)
		return "update identical"
	case 13:
		before := len(h.model)
		for _, id := range h.ids() { // sorted: the seed must reproduce the run
			row := h.model[id]
			if row.id%m == r && h.rng.Intn(3) == 0 {
				delete(h.model, id)
			}
		}
		// Delete exactly the rows the model dropped.
		keep := make([]string, 0, len(h.model))
		for id := range h.model {
			keep = append(keep, strconv.Itoa(id))
		}
		sort.Strings(keep)
		h.mustExec(fmt.Sprintf(`DELETE FROM docs WHERE id NOT IN (%s)`, strings.Join(keep, ", ")))
		if len(h.model) < before {
			// The next HNSW search rebuilds the graph from the surviving rows, or
			// follows the removal (followVecHNSWIndex): it then drops the dead nodes
			// and renumbers the others, which keeps the old structure of a graph that
			// is not the one a build over the surviving rows would produce, so its
			// answers are approximate like those of a graph with moved vectors.
			// compare notices a follow through vecHNSWFollowCount; a rebuild leaves
			// the strict comparison in force.
			h.annMoved = false
			h.followSeen = vecHNSWFollowCount.Load()
		}
		return fmt.Sprintf("delete %%%d=%d", m, r)
	case 14:
		h.mustExec(`DELETE FROM docs WHERE id < 0`)
		return "delete nothing"
	default:
		// A statement that fails after it already rewrote some rows: nothing may stick.
		victim := -1
		ids := h.ids()
		if len(ids) > 3 {
			victim = ids[len(ids)*2/3]
		}
		_, err := h.exec(h.db, h.tenant, fmt.Sprintf(`UPDATE docs SET content = 'ghost phantom', embedding = '%s', loaded_at = 1 / (id - %d)`, retrievalVecLiteral(h.randVec()), victim))
		if err == nil && victim >= 0 {
			h.t.Fatalf("%s: the failing statement succeeded", h.label)
		}
		h.annMoved = true
		return "failed statement"
	}
}

func (h *retrievalDiffHarness) ids() []int {
	ids := make([]int, 0, len(h.model))
	for id := range h.model {
		ids = append(ids, id)
	}
	sort.Ints(ids)
	return ids
}

// liveRows reads the live table in physical order.
func (h *retrievalDiffHarness) liveRows() []*retrievalDiffRow {
	rs := h.mustExec(`SELECT id, loaded_at, content, embedding FROM docs`)
	rows := make([]*retrievalDiffRow, 0, len(rs.Rows))
	for _, r := range rs.Rows {
		id, _ := toInt(r["id"])
		loaded, _ := toInt(r["loaded_at"])
		content, _ := r["content"].(string)
		vec, ok := r["embedding"].([]float64)
		if !ok {
			h.t.Fatalf("%s: embedding is %T", h.label, r["embedding"])
		}
		rows = append(rows, &retrievalDiffRow{id: id, loaded: loaded, content: content, vec: vec})
	}
	return rows
}

func (h *retrievalDiffHarness) checkModel(rows []*retrievalDiffRow) {
	h.t.Helper()
	if len(rows) != len(h.model) {
		h.t.Fatalf("%s: live table has %d rows, model %d", h.label, len(rows), len(h.model))
	}
	for _, row := range rows {
		want := h.model[row.id]
		if want == nil || want.loaded != row.loaded || want.content != row.content || !reflect.DeepEqual(want.vec, row.vec) {
			h.t.Fatalf("%s: row %d is %+v, model says %+v", h.label, row.id, row, want)
		}
	}
}

type retrievalDiffQuery struct {
	sql string
	// exact is the flat (exact) equivalent of an approximate HNSW query, used
	// to check that every score an approximate answer reports is the true one.
	exact string
}

func (h *retrievalDiffHarness) queries() []retrievalDiffQuery {
	vec := func() string { return retrievalVecLiteral(h.randVec()) }
	terms := []string{
		fmt.Sprintf("w%d", h.rng.Intn(24)),
		fmt.Sprintf("w%d OR w%d", h.rng.Intn(24), h.rng.Intn(24)),
		fmt.Sprintf("w%d AND tag%d", h.rng.Intn(24), h.rng.Intn(6)),
		fmt.Sprintf("rare%d OR w%d", h.rng.Intn(400), h.rng.Intn(24)),
		fmt.Sprintf("\"w%d w%d\"", h.rng.Intn(24), h.rng.Intn(24)),
		"ghost",
	}
	var qs []retrievalDiffQuery
	for _, term := range terms {
		qs = append(qs, retrievalDiffQuery{sql: fmt.Sprintf(`SELECT * FROM FTS_SEARCH('docs', '%s', 8, 'content')`, term)})
	}
	for i := 0; i < 2; i++ {
		q := vec()
		qs = append(qs,
			retrievalDiffQuery{sql: fmt.Sprintf(`SELECT * FROM VEC_SEARCH('docs', 'embedding', '%s', 6, 'cosine')`, vec())},
			retrievalDiffQuery{
				sql:   fmt.Sprintf(`SELECT * FROM VEC_SEARCH('docs', 'embedding', '%s', 6, 'cosine', 'hnsw')`, q),
				exact: fmt.Sprintf(`SELECT * FROM VEC_SEARCH('docs', 'embedding', '%s', 100000, 'cosine')`, q),
			},
			retrievalDiffQuery{sql: fmt.Sprintf(`SELECT * FROM VEC_SEARCH('docs', 'embedding', '%s', 5, 'l2')`, vec())},
		)
	}
	qs = append(qs,
		retrievalDiffQuery{sql: fmt.Sprintf(`SELECT * FROM HYBRID_SEARCH('docs', 'embedding', 'content', '%s', VEC_FROM_JSON('%s'), 6, '{"key_columns":["id"]}')`, terms[1], vec())},
		retrievalDiffQuery{sql: fmt.Sprintf(`SELECT * FROM HYBRID_SEARCH('docs', 'embedding', 'content', '%s', VEC_FROM_JSON('%s'), 6, '{"key_columns":["id"]}')`, terms[0], vec())},
	)
	return qs
}

// compare builds a cold database from the live rows (same physical order) and
// requires every search to answer identically. HNSW is approximate by design: a
// graph that has seen its vectors updated keeps stale links and merges the
// moved rows exactly, so it may legitimately answer differently from a freshly
// built one (the baseline behaves the same); so may a graph that followed a
// DELETE, which drops the dead nodes and edges instead of being rebuilt. For such
// a graph the answer must still consist of live rows with their exact scores
// (vector_hnsw_follow_test.go measures recall); for a graph whose vectors never
// moved and that never lost a row (inserts and bookkeeping updates only) it must
// equal the cold graph's answer bit for bit.
func (h *retrievalDiffHarness) compare(step int, what string) {
	h.t.Helper()
	rows := h.liveRows()
	h.checkModel(rows)
	coldTenant := fmt.Sprintf("diffcold-%s-%d", h.label, step)
	cold := storage.NewDB()
	run := func(db *storage.DB, tenant, sql string) *ResultSet {
		rs, err := h.exec(db, tenant, sql)
		if err != nil {
			h.t.Fatalf("%s step %d (%s): %s: %v", h.label, step, what, sql, err)
		}
		return rs
	}
	run(cold, coldTenant, `CREATE TABLE docs (id INT, loaded_at INT, content TEXT, embedding VECTOR)`)
	for _, row := range rows {
		run(cold, coldTenant, fmt.Sprintf(`INSERT INTO docs VALUES (%d, %d, '%s', '%s')`, row.id, row.loaded, row.content, retrievalVecLiteral(row.vec)))
	}
	for _, q := range h.queries() {
		got := run(h.db, h.tenant, q.sql)
		if q.exact != "" && vecHNSWFollowCount.Load() != h.followSeen {
			// A removal was followed since the last look: the graph is no longer
			// what a build over the rows would produce.
			h.annMoved = true
			h.followSeen = vecHNSWFollowCount.Load()
		}
		want := run(cold, coldTenant, q.sql)
		if q.exact != "" && h.annMoved {
			exact := map[any]any{}
			for _, r := range run(h.db, h.tenant, q.exact).Rows {
				exact[r["id"]] = r["_vec_distance"]
			}
			for _, r := range got.Rows {
				if d, ok := exact[r["id"]]; !ok || d != r["_vec_distance"] {
					h.t.Fatalf("%s step %d (%s): approximate answer for\n  %s\nreports row %v with distance %v, the exact distance is %v (present=%v)", h.label, step, what, q.sql, r["id"], r["_vec_distance"], d, ok)
				}
			}
			continue
		}
		if !reflect.DeepEqual(got.Rows, want.Rows) {
			h.t.Fatalf("%s step %d (%s): live and cold answers differ for\n  %s\nlive: %v\ncold: %v", h.label, step, what, q.sql, got.Rows, want.Rows)
		}
	}
	purgeFTSCachesFor(coldTenant, "docs")
	purgeVectorCachesFor(coldTenant, "docs")
}

func (h *retrievalDiffHarness) warm() {
	switch h.rng.Intn(4) {
	case 0:
		h.mustExec(`SELECT * FROM FTS_WARM('docs', 'content')`)
	case 1:
		h.mustExec(`SELECT * FROM VEC_WARM('docs', 'embedding', 'cosine', 'hnsw')`)
	case 2:
		h.mustExec(`SELECT * FROM VEC_WARM('docs', 'embedding')`)
	}
}

func runRetrievalDifferential(t *testing.T, seed int64, steps int, reopen bool) {
	runRetrievalDifferentialOpts(t, seed, steps, reopen, false)
}

func runRetrievalDifferentialOpts(t *testing.T, seed int64, steps int, reopen, structural bool) {
	h := &retrievalDiffHarness{
		t: t, rng: rand.New(rand.NewSource(seed)), tenant: fmt.Sprintf("difflive-%d-%t", seed, reopen),
		model: map[int]*retrievalDiffRow{}, label: fmt.Sprintf("seed %d reopen=%t", seed, reopen), structural: structural,
	}
	if reopen {
		h.dir = filepath.Join(t.TempDir(), "db")
	}
	h.open()
	t.Cleanup(func() {
		if h.db != nil {
			_ = h.db.Close()
		}
		purgeFTSCachesFor(h.tenant, "docs")
		purgeVectorCachesFor(h.tenant, "docs")
	})
	h.mustExec(`CREATE TABLE docs (id INT, loaded_at INT, content TEXT, embedding VECTOR)`)
	h.insert(140)
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
		if reopen && step%17 == 0 {
			// Sync, drop every runtime cache by closing, and carry on from disk:
			// the persisted FTS index and HNSW graph must still be correct.
			if err := h.db.Sync(); err != nil {
				t.Fatal(err)
			}
			purgeFTSCachesFor(h.tenant, "docs")
			purgeVectorCachesFor(h.tenant, "docs")
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

func TestRetrievalMaintenanceMatchesColdBuild(t *testing.T) {
	for _, seed := range []int64{1, 2, 3, 4, 5, 6} {
		t.Run(fmt.Sprintf("memory/seed%d", seed), func(t *testing.T) {
			runRetrievalDifferential(t, seed, 60, false)
		})
	}
}

func TestRetrievalMaintenanceMatchesColdBuildAcrossReopen(t *testing.T) {
	for _, seed := range []int64{11, 12} {
		t.Run(fmt.Sprintf("disk/seed%d", seed), func(t *testing.T) {
			runRetrievalDifferential(t, seed, 60, true)
		})
	}
}
