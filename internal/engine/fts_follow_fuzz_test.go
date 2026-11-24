package engine

import (
	"context"
	"fmt"
	"math/rand"
	"reflect"
	"slices"
	"strings"
	"testing"
	"unicode"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// This file checks the incremental maintenance of a persistent FTS index across
// DELETE, UPDATE, INSERT, rollbacks, transactions and reopen against the only
// reference that cannot be wrong: an index built from scratch over the very same
// rows. The two are compared canonically (by term text, not by term id or arena
// offset), by their structural invariants, and by what searches return.

// ---------------------------------------------------------------------------
// Canonical comparison and invariants

func ftsDecodeTokens(index *storage.FTSIndex, names []string, doc storage.FTSDocument) []string {
	if !doc.Valid {
		return nil
	}
	ids := index.DocTokenIDs[doc.TokenStart : doc.TokenStart+doc.TokenCount]
	out := make([]string, len(ids))
	for i, id := range ids {
		out[i] = names[id]
	}
	return out
}

// ftsCheckInvariants verifies the structure of index over a table of rows rows.
func ftsCheckInvariants(t *testing.T, index *storage.FTSIndex, rows int, label string) {
	t.Helper()
	fail := func(format string, args ...any) {
		t.Helper()
		t.Fatalf("%s: "+format, append([]any{label}, args...)...)
	}
	if len(index.Docs) != rows || index.BuiltRows != rows {
		fail("docs=%d builtRows=%d, table has %d rows", len(index.Docs), index.BuiltRows, rows)
	}
	if !index.Verified() {
		fail("index is not verified")
	}
	if !ftsValidateStructure(index) {
		fail("structural validation failed")
	}
	names := index.TermNames()
	if len(names) != len(index.TermIDs) {
		fail("%d term names for %d terms", len(names), len(index.TermIDs))
	}
	for term, id := range index.TermIDs {
		if id < 0 || int(id) >= len(names) || names[id] != term {
			fail("term %q has id %d but names[id] is wrong", term, id)
		}
	}
	liveTokens, liveTerms, valid := 0, 0, 0
	docFreq := make(map[int32]int)
	totalLen := 0.0
	for pos, doc := range index.Docs {
		if !doc.Valid {
			continue
		}
		valid++
		if doc.TermStart < 0 || doc.TermCount < 0 || int(doc.TermStart+doc.TermCount) > len(index.DocTermIDs) ||
			doc.TokenStart < 0 || doc.TokenCount < 0 || int(doc.TokenStart+doc.TokenCount) > len(index.DocTokenIDs) {
			fail("doc %d has arena offsets out of range", pos)
		}
		if doc.TokenCount == 0 || doc.DocLen != float64(doc.TokenCount) {
			fail("doc %d: tokenCount=%d docLen=%v", pos, doc.TokenCount, doc.DocLen)
		}
		liveTokens += int(doc.TokenCount)
		liveTerms += int(doc.TermCount)
		totalLen += doc.DocLen
		counts := map[int32]int32{}
		for _, id := range index.DocTokenIDs[doc.TokenStart : doc.TokenStart+doc.TokenCount] {
			counts[id]++
		}
		run := index.DocTermIDs[doc.TermStart : doc.TermStart+doc.TermCount]
		runCounts := index.DocTermCounts[doc.TermStart : doc.TermStart+doc.TermCount]
		if len(run) != len(counts) {
			fail("doc %d: %d distinct terms in its tokens, %d in its term run", pos, len(counts), len(run))
		}
		for i, id := range run {
			if i > 0 && run[i-1] >= id {
				fail("doc %d: term run not strictly ascending", pos)
			}
			if counts[id] != runCounts[i] {
				fail("doc %d: term %d counted %d, run says %d", pos, id, counts[id], runCounts[i])
			}
			docFreq[id]++
		}
	}
	if index.NumDocs != valid || index.TotalDocLen != totalLen {
		fail("NumDocs=%d TotalDocLen=%v, documents say %d and %v", index.NumDocs, index.TotalDocLen, valid, totalLen)
	}
	if valid > 0 && index.AvgDocLen != index.TotalDocLen/float64(valid) {
		fail("AvgDocLen=%v, want %v", index.AvgDocLen, index.TotalDocLen/float64(valid))
	}
	if valid == 0 && index.AvgDocLen != 0 {
		fail("AvgDocLen=%v for an index without documents", index.AvgDocLen)
	}
	if index.StaleTokens != len(index.DocTokenIDs)-liveTokens || index.StaleTerms != len(index.DocTermIDs)-liveTerms {
		fail("stale tokens/terms %d/%d, arenas say %d/%d", index.StaleTokens, index.StaleTerms,
			len(index.DocTokenIDs)-liveTokens, len(index.DocTermIDs)-liveTerms)
	}
	for term, list := range index.Postings {
		counts := index.PostingCounts[term]
		if len(counts) != len(list) {
			fail("%q: %d rows, %d counts", term, len(list), len(counts))
		}
		blocks := index.PostingBlocks[term]
		if want := (len(list) + storage.FTSPostingBlockSize - 1) / storage.FTSPostingBlockSize; len(blocks) != want {
			fail("%q: %d blocks for %d entries, want %d", term, len(blocks), len(list), want)
		}
		id, known := index.TermIDs[term]
		if !known && len(list) > 0 {
			fail("%q has postings but no term id", term)
		}
		if len(list) != docFreq[id] {
			fail("%q: df %d, documents say %d", term, len(list), docFreq[id])
		}
		for i, row := range list {
			if row < 0 || int(row) >= rows || (i > 0 && list[i-1] >= row) {
				fail("%q: entry %d is %d (table has %d rows) or not ascending", term, i, row, rows)
			}
			doc := index.Docs[row]
			if !doc.Valid {
				fail("%q lists document %d which holds nothing", term, row)
			}
			block := blocks[i/storage.FTSPostingBlockSize]
			if block.MaxFrequency < counts[i] || block.MinDocLen > doc.DocLen {
				fail("%q: unsafe block bound at entry %d", term, i)
			}
		}
	}
	for id, n := range docFreq {
		if len(index.Postings[names[id]]) != n {
			fail("term %q is in %d documents but its list has %d entries", names[id], n, len(index.Postings[names[id]]))
		}
	}
}

// ftsCompareCanonical checks got (maintained) against want (built from scratch
// over the same rows), comparing everything by term text.
func ftsCompareCanonical(t *testing.T, got, want *storage.FTSIndex, label string) {
	t.Helper()
	if got.NumDocs != want.NumDocs || got.TotalDocLen != want.TotalDocLen || got.AvgDocLen != want.AvgDocLen ||
		got.BuiltRows != want.BuiltRows || len(got.Docs) != len(want.Docs) {
		t.Fatalf("%s: statistics differ: docs %d/%d total %v/%v avg %v/%v rows %d/%d", label,
			got.NumDocs, want.NumDocs, got.TotalDocLen, want.TotalDocLen, got.AvgDocLen, want.AvgDocLen, got.BuiltRows, want.BuiltRows)
	}
	gotNames, wantNames := got.TermNames(), want.TermNames()
	for pos := range want.Docs {
		gd, wd := got.Docs[pos], want.Docs[pos]
		if gd.Valid != wd.Valid || gd.DocLen != wd.DocLen || gd.TermCount != wd.TermCount || gd.TokenCount != wd.TokenCount {
			t.Fatalf("%s: document %d differs: %+v vs %+v", label, pos, gd, wd)
		}
		if gt, wt := ftsDecodeTokens(got, gotNames, gd), ftsDecodeTokens(want, wantNames, wd); !slices.Equal(gt, wt) {
			t.Fatalf("%s: tokens of document %d differ: %v vs %v", label, pos, gt, wt)
		}
	}
	for term, wl := range want.Postings {
		if len(wl) == 0 {
			continue
		}
		if !slices.Equal(got.Postings[term], wl) || !slices.Equal(got.PostingCounts[term], want.PostingCounts[term]) {
			t.Fatalf("%s: postings of %q differ: %v/%v vs %v/%v", label, term, got.Postings[term], got.PostingCounts[term], wl, want.PostingCounts[term])
		}
		if !reflect.DeepEqual(got.PostingBlocks[term], want.PostingBlocks[term]) {
			t.Fatalf("%s: posting blocks of %q differ: %v vs %v", label, term, got.PostingBlocks[term], want.PostingBlocks[term])
		}
	}
	for term, gl := range got.Postings {
		if len(gl) > 0 && len(want.Postings[term]) == 0 {
			t.Fatalf("%s: %q has postings %v that a fresh build does not", label, term, gl)
		}
	}
}

// ---------------------------------------------------------------------------
// The workload

type fzRow struct {
	id, src int
	body    any // string or nil
	note    string
}

type fzState struct {
	rows    []fzRow
	nextID  int
	nextSrc int
}

func (s *fzState) clone() *fzState {
	c := *s
	c.rows = slices.Clone(s.rows)
	return &c
}

var (
	fzWords    = []string{"alpha", "beta", "gamma", "delta", "epsilon", "zeta", "eta", "theta", "iota", "kappa", "lambda", "omega", "sigma", "tau"}
	fzUnicode  = []string{"grüße", "naïve", "café", "日本語", "ÄÖÜ", "straße", "resumé", "ñandú", "Ünïcödé"}
	fzPunct    = []string{"", "", "", ",", ".", "!", ")", "-x"}
	fzOverlaps = []string{"x-life", "don''t", "e-mail", "3.5mm", "A/B", "O-ring"}
)

func fzWord(r *rand.Rand, vocab int) string {
	switch k := r.Intn(30); {
	case k == 0:
		return fzUnicode[r.Intn(len(fzUnicode))]
	case k == 1:
		return fzOverlaps[r.Intn(len(fzOverlaps))]
	case k < 10:
		return fzWords[r.Intn(len(fzWords))] // frequent words
	default:
		x := r.Float64()
		return fmt.Sprintf("w%d", int(float64(vocab)*x*x)) // skewed numbered vocabulary
	}
}

// fzText returns a SQL literal (already quoted) or NULL, and the Go value.
func fzText(r *rand.Rand, vocab int) any {
	switch k := r.Intn(40); {
	case k == 0:
		return nil
	case k == 1:
		return ""
	case k == 2:
		return "   ,, ... "
	}
	var n int
	switch k := r.Intn(20); {
	case k == 0:
		n = 100 + r.Intn(200)
	case k < 6:
		n = 1 + r.Intn(3)
	default:
		n = 4 + r.Intn(25)
	}
	var sb strings.Builder
	for i := 0; i < n; i++ {
		w := fzWord(r, vocab)
		if r.Intn(7) == 0 {
			w = strings.ToUpper(w)
		}
		sb.WriteString(w)
		sb.WriteString(fzPunct[r.Intn(len(fzPunct))])
		sb.WriteByte(' ')
	}
	text := strings.ReplaceAll(sb.String(), "''", "'")
	return text
}

func fzLit(v any) string {
	if v == nil {
		return "NULL"
	}
	return sqlQuote(v.(string))
}

func fzVec(r *rand.Rand) string {
	return fmt.Sprintf("'[%.3f,%.3f,%.3f,%.3f]'", r.Float64(), r.Float64(), r.Float64(), r.Float64())
}

type fzOp struct {
	sql    string
	fails  bool
	mirror func(*fzState)
}

func fzInsertOp(r *rand.Rand, s *fzState, vocab int, bulk bool) fzOp {
	k := 1 + r.Intn(8)
	if bulk {
		k = 70 + r.Intn(80)
	}
	src := s.nextSrc
	s.nextSrc++
	var rows []fzRow
	var values []string
	for i := 0; i < k; i++ {
		row := fzRow{id: s.nextID, src: src, body: fzText(r, vocab), note: fmt.Sprintf("n%d", r.Intn(5))}
		s.nextID++
		rows = append(rows, row)
		values = append(values, fmt.Sprintf("(%d, %d, %s, '%s', %s)", row.id, row.src, fzLit(row.body), row.note, fzVec(r)))
	}
	return fzOp{sql: "INSERT INTO fz VALUES " + strings.Join(values, ", "), mirror: func(m *fzState) { m.rows = append(m.rows, rows...) }}
}

func fzPickRow(r *rand.Rand, s *fzState) (fzRow, bool) {
	if len(s.rows) == 0 {
		return fzRow{}, false
	}
	return s.rows[r.Intn(len(s.rows))], true
}

func fzUpdateOp(r *rand.Rand, s *fzState, vocab int) fzOp {
	row, ok := fzPickRow(r, s)
	if !ok {
		return fzOp{sql: "UPDATE fz SET note = 'none' WHERE id = -1", mirror: func(*fzState) {}}
	}
	id, src := row.id, row.src
	switch r.Intn(9) {
	case 0: // text of one row
		body := fzText(r, vocab)
		return fzOp{sql: fmt.Sprintf("UPDATE fz SET body = %s WHERE id = %d", fzLit(body), id), mirror: func(m *fzState) {
			for i := range m.rows {
				if m.rows[i].id == id {
					m.rows[i].body = body
				}
			}
		}}
	case 1: // a column the index does not cover, a whole source
		note := fmt.Sprintf("u%d", r.Intn(9))
		return fzOp{sql: fmt.Sprintf("UPDATE fz SET note = '%s' WHERE src = %d", note, src), mirror: func(m *fzState) {
			for i := range m.rows {
				if m.rows[i].src == src {
					m.rows[i].note = note
				}
			}
		}}
	case 2: // both
		body, note := fzText(r, vocab), fmt.Sprintf("b%d", r.Intn(9))
		return fzOp{sql: fmt.Sprintf("UPDATE fz SET body = %s, note = '%s' WHERE id = %d", fzLit(body), note, id), mirror: func(m *fzState) {
			for i := range m.rows {
				if m.rows[i].id == id {
					m.rows[i].body, m.rows[i].note = body, note
				}
			}
		}}
	case 3: // the same text
		return fzOp{sql: fmt.Sprintf("UPDATE fz SET body = %s WHERE id = %d", fzLit(row.body), id), mirror: func(*fzState) {}}
	case 4: // matches nothing
		return fzOp{sql: "UPDATE fz SET body = 'never stored' WHERE id = -1", mirror: func(*fzState) {}}
	case 5: // rewrite a whole source
		body := fzText(r, vocab)
		return fzOp{sql: fmt.Sprintf("UPDATE fz SET body = %s WHERE src = %d", fzLit(body), src), mirror: func(m *fzState) {
			for i := range m.rows {
				if m.rows[i].src == src {
					m.rows[i].body = body
				}
			}
		}}
	case 6: // a bookkeeping touch of every row
		note := fmt.Sprintf("t%d", r.Intn(9))
		return fzOp{sql: fmt.Sprintf("UPDATE fz SET note = '%s'", note), mirror: func(m *fzState) {
			for i := range m.rows {
				m.rows[i].note = note
			}
		}}
	case 7: // a few scattered rows
		body := fzText(r, vocab)
		mod := 3 + r.Intn(5)
		res := r.Intn(mod)
		return fzOp{sql: fmt.Sprintf("UPDATE fz SET body = %s WHERE id %% %d = %d", fzLit(body), mod, res), mirror: func(m *fzState) {
			for i := range m.rows {
				if m.rows[i].id%mod == res {
					m.rows[i].body = body
				}
			}
		}}
	default: // NULL out
		return fzOp{sql: fmt.Sprintf("UPDATE fz SET body = NULL WHERE id = %d", id), mirror: func(m *fzState) {
			for i := range m.rows {
				if m.rows[i].id == id {
					m.rows[i].body = nil
				}
			}
		}}
	}
}

func fzDeleteOp(r *rand.Rand, s *fzState) fzOp {
	drop := func(match func(fzRow) bool) func(*fzState) {
		return func(m *fzState) {
			kept := m.rows[:0:0]
			for _, row := range m.rows {
				if !match(row) {
					kept = append(kept, row)
				}
			}
			m.rows = kept
		}
	}
	if len(s.rows) == 0 {
		return fzOp{sql: "DELETE FROM fz WHERE id = -1", mirror: func(*fzState) {}}
	}
	switch r.Intn(11) {
	case 0, 1: // a random set
		ids := map[int]bool{}
		var list []string
		for i := 0; i < 1+r.Intn(6); i++ {
			id := s.rows[r.Intn(len(s.rows))].id
			if !ids[id] {
				ids[id] = true
				list = append(list, fmt.Sprint(id))
			}
		}
		return fzOp{sql: "DELETE FROM fz WHERE id IN (" + strings.Join(list, ", ") + ")", mirror: drop(func(row fzRow) bool { return ids[row.id] })}
	case 2: // the first row
		id := s.rows[0].id
		return fzOp{sql: fmt.Sprintf("DELETE FROM fz WHERE id = %d", id), mirror: drop(func(row fzRow) bool { return row.id == id })}
	case 3: // the last row
		id := s.rows[len(s.rows)-1].id
		return fzOp{sql: fmt.Sprintf("DELETE FROM fz WHERE id = %d", id), mirror: drop(func(row fzRow) bool { return row.id == id })}
	case 4: // everything, without a predicate
		return fzOp{sql: "DELETE FROM fz", mirror: drop(func(fzRow) bool { return true })}
	case 5: // everything, with one
		if r.Intn(3) != 0 {
			return fzOp{sql: "DELETE FROM fz WHERE id = -7", mirror: func(*fzState) {}}
		}
		return fzOp{sql: "DELETE FROM fz WHERE id >= 0", mirror: drop(func(fzRow) bool { return true })}
	case 6: // nothing
		return fzOp{sql: "DELETE FROM fz WHERE src < 0", mirror: func(*fzState) {}}
	case 7, 8: // a source worth of adjacent rows
		src := s.rows[r.Intn(len(s.rows))].src
		return fzOp{sql: fmt.Sprintf("DELETE FROM fz WHERE src = %d", src), mirror: drop(func(row fzRow) bool { return row.src == src })}
	case 9: // a complex predicate
		mod := 4 + r.Intn(6)
		res := r.Intn(mod)
		return fzOp{sql: fmt.Sprintf("DELETE FROM fz WHERE (id * 3) %% %d = %d", mod, res), mirror: drop(func(row fzRow) bool { return (row.id*3)%mod == res })}
	default: // several sources
		a, b := s.rows[r.Intn(len(s.rows))].src, s.rows[r.Intn(len(s.rows))].src
		return fzOp{sql: fmt.Sprintf("DELETE FROM fz WHERE src = %d OR src = %d", a, b), mirror: drop(func(row fzRow) bool { return row.src == a || row.src == b })}
	}
}

// fzFailingOp is a statement that mutates and then fails, so the executor rolls it back.
func fzFailingOp(r *rand.Rand, s *fzState) fzOp {
	if len(s.rows) == 0 {
		// Nothing to delete, so nothing would be projected and nothing fails.
		return fzOp{sql: "DELETE FROM fz WHERE id = -1", mirror: func(*fzState) {}}
	}
	row := s.rows[r.Intn(len(s.rows))]
	switch r.Intn(4) {
	case 0: // one row by id: the single-row DELETE snapshot, general scan because of RETURNING
		return fzOp{sql: fmt.Sprintf("DELETE FROM fz WHERE id = %d RETURNING missing_column", row.id), fails: true, mirror: func(*fzState) {}}
	case 1: // the same with a predicate the point path does not take
		return fzOp{sql: fmt.Sprintf("DELETE FROM fz WHERE id = %d AND LENGTH(note) >= 0 RETURNING missing_column", row.id), fails: true, mirror: func(*fzState) {}}
	}
	return fzOp{sql: fmt.Sprintf("DELETE FROM fz WHERE src = %d RETURNING missing_column", row.src), fails: true, mirror: func(*fzState) {}}
}

// fzVariant is the shape of the table the workload runs against.
type fzVariant struct {
	pk       bool // id is a PRIMARY KEY: DELETE ... WHERE id = k is a point delete (rows reorder)
	srcIndex bool // a secondary index on src
	multi    bool // the FTS index covers body and note
}

type fzHarness struct {
	t       *testing.T
	r       *rand.Rand
	db      *storage.DB
	refDB   *storage.DB
	disk    bool
	path    string
	state   *fzState
	vocab   int
	cols    []int
	key     string
	variant fzVariant
	seen    []string // words of every text ever inserted, deleted or not
	seenID  int      // rows with a smaller id were scanned for words already
	stats   struct{ followed, rebuilt, removalsFollowed, removalsRebuilt, pureRemovalRebuilds int }
	checks  int
}

func newFZ(t *testing.T, seed int64, disk bool, variant ...fzVariant) *fzHarness {
	t.Helper()
	h := &fzHarness{t: t, r: rand.New(rand.NewSource(seed)), disk: disk, state: &fzState{}, cols: []int{2}}
	if len(variant) > 0 {
		h.variant = variant[0]
	}
	if h.variant.multi {
		h.cols = []int{2, 3}
	}
	h.key = ftsColsCacheKey(h.cols)
	h.vocab = 30 + h.r.Intn(300)
	if disk {
		h.path = t.TempDir() + "/db"
		db, err := storage.OpenDB(storage.StorageConfig{Mode: storage.ModeDisk, Path: h.path})
		if err != nil {
			t.Fatal(err)
		}
		h.db = db
	} else {
		h.db = storage.NewDB()
	}
	t.Cleanup(func() { _ = h.db.Close() })
	idType := "INT"
	if h.variant.pk {
		idType = "INT PRIMARY KEY"
	}
	h.exec(h.db, `CREATE TABLE fz (id `+idType+`, src INT, body TEXT, note TEXT, embedding VECTOR)`)
	if h.variant.srcIndex {
		h.exec(h.db, `CREATE INDEX fz_src ON fz(src)`)
	}
	rows := 300 + h.r.Intn(2700)
	for len(h.state.rows) < rows {
		op := fzInsertOp(h.r, h.state, h.vocab, h.r.Intn(3) == 0)
		h.exec(h.db, op.sql)
		op.mirror(h.state)
	}
	h.noteWords()
	return h
}

// noteWords remembers the words of the rows inserted since the last call, for
// prefix queries over terms that may no longer exist.
func (h *fzHarness) noteWords() {
	for _, row := range h.state.rows {
		if row.id < h.seenID || len(h.seen) > 4000 {
			continue
		}
		if text, ok := row.body.(string); ok {
			for _, w := range fzTokens(text) {
				if len(w) >= 3 {
					h.seen = append(h.seen, w)
				}
			}
		}
	}
	h.seenID = h.state.nextID
}

// fzTokens splits text into lower-case words the way a reader would.
func fzTokens(text string) []string {
	return strings.FieldsFunc(strings.ToLower(text), func(r rune) bool { return !unicode.IsLetter(r) && !unicode.IsDigit(r) })
}

func (h *fzHarness) exec(db *storage.DB, sql string) *ResultSet {
	h.t.Helper()
	rs, err := Execute(context.Background(), db, "default", mustParse(sql))
	if err != nil {
		h.t.Fatalf("SQL failed: %.200s\n  error: %v", sql, err)
	}
	return rs
}

func (h *fzHarness) table() *storage.Table {
	h.t.Helper()
	table, err := h.db.Get("default", "fz")
	if err != nil {
		h.t.Fatal(err)
	}
	return table
}

// apply runs one operation against db and, when it succeeds, against state.
func (h *fzHarness) apply(db *storage.DB, state *fzState, op fzOp) {
	h.t.Helper()
	_, err := Execute(context.Background(), db, "default", mustParse(op.sql))
	switch {
	case op.fails && err == nil:
		h.t.Fatalf("expected %.100s to fail", op.sql)
	case !op.fails && err != nil:
		h.t.Fatalf("SQL failed: %.200s\n  error: %v", op.sql, err)
	}
	if err == nil && op.mirror != nil {
		op.mirror(state)
	}
}

// randomOp draws one mutating statement together with its effect on the mirror.
func (h *fzHarness) randomOp(state *fzState) fzOp {
	switch k := h.r.Intn(20); {
	case k < 4:
		return fzInsertOp(h.r, state, h.vocab, h.r.Intn(12) == 0)
	case k < 10:
		return fzUpdateOp(h.r, state, h.vocab)
	case k < 17:
		return fzDeleteOp(h.r, state)
	default:
		return fzFailingOp(h.r, state)
	}
}

// searchShadow searches the transaction's private copy of the table, whose
// index and change log were cloned from the live ones, and compares it with a
// build from scratch.
func (h *fzHarness) searchShadow(shadow *storage.DB, label string) {
	h.t.Helper()
	table, err := shadow.Get("default", "fz")
	if err != nil {
		h.t.Fatal(err)
	}
	refDB, refTable := h.referenceOf(table)
	_ = getFTSDocCache("default", table, h.cols)
	got := table.FTSIndexes[h.key]
	if got == nil {
		h.t.Fatalf("%s: no index in the transaction", label)
	}
	_ = getFTSDocCache("ref", refTable, h.cols)
	ftsCheckInvariants(h.t, got, len(table.Rows), label)
	ftsCompareCanonical(h.t, got, refTable.FTSIndexes[h.key], label)
	h.compareQueries(label, shadow, refDB, 6)
}

func (h *fzHarness) transaction() {
	if h.r.Intn(2) == 0 {
		h.followIndex("before a transaction") // so the copy has an index to carry along
	}
	h.db.LockContentForRead()
	base, shadow, release := h.db.SnapshotForTxShared()
	h.db.UnlockContentForRead()
	txState := h.state.clone()
	for i, n := 0, 1+h.r.Intn(4); i < n; i++ {
		h.apply(shadow, txState, h.randomOp(txState))
		if h.r.Intn(3) == 0 {
			h.searchShadow(shadow, fmt.Sprintf("in a transaction, statement %d", i))
		}
	}
	if h.r.Intn(2) == 0 {
		h.db.LockContentForWrite()
		changes := storage.CollectWALChanges(base, shadow)
		err := h.db.ApplyWALChanges(changes)
		h.db.UnlockContentForWrite()
		if err != nil {
			h.t.Fatal(err)
		}
		h.state = txState
	}
	release()
}

func (h *fzHarness) reopen() {
	h.t.Helper()
	if err := h.db.Close(); err != nil {
		h.t.Fatal(err)
	}
	db, err := storage.OpenDB(storage.StorageConfig{Mode: storage.ModeDisk, Path: h.path})
	if err != nil {
		h.t.Fatal(err)
	}
	h.db = db
	h.t.Cleanup(func() { _ = db.Close() })
}

func (h *fzHarness) checkRows(label string) {
	h.t.Helper()
	table := h.table()
	if len(table.Rows) != len(h.state.rows) {
		h.t.Fatalf("%s: table has %d rows, the mirror %d", label, len(table.Rows), len(h.state.rows))
	}
	if h.variant.pk {
		// A point DELETE moves the last row into the freed slot, so only the set
		// of rows is defined, not their order.
		byID := make(map[int]fzRow, len(h.state.rows))
		for _, want := range h.state.rows {
			byID[want.id] = want
		}
		for i, row := range table.Rows {
			want, ok := byID[row[0].(int)]
			if !ok || row[2] != want.body || row[3] != want.note {
				h.t.Fatalf("%s: row %d is %v, mirror says %+v (known: %v)", label, i, row[:4], want, ok)
			}
		}
		return
	}
	for i, row := range table.Rows {
		want := h.state.rows[i]
		if row[0] != want.id || row[2] != want.body || row[3] != want.note {
			h.t.Fatalf("%s: row %d is %v, mirror says %+v", label, i, row[:4], want)
		}
	}
}

// followIndex asks for the document cache like a search does and records whether
// the existing index was carried forward or replaced.
func (h *fzHarness) followIndex(label string) *storage.FTSIndex {
	h.t.Helper()
	table := h.table()
	before := table.FTSIndexes[h.key]
	removals, pureRemoval, predictedRebuild := false, false, false
	if before != nil && before.StructVersion != table.StructVersion() {
		changes, ok := table.ChangesSince(before.StructVersion, before.BuiltRows, storage.ColumnMask(h.cols...))
		if ok && len(changes.Dead) > 0 {
			removals = true
			pureRemoval = len(changes.Updated) == 0
			// The cases in which giving up on the index is the documented choice.
			predictedRebuild = len(changes.Updated) > ftsRefreshRowLimit(len(table.Rows)) ||
				ftsRemovalNeedsCompaction(before, changes.Dead)
		}
	}
	_ = getFTSDocCache("default", table, h.cols)
	after := table.FTSIndexes[h.key]
	if after == nil {
		h.t.Fatalf("%s: no index after a search", label)
	}
	if before != nil && before == after {
		h.stats.followed++
		if removals {
			h.stats.removalsFollowed++
		}
	} else if before != nil {
		h.stats.rebuilt++
		if removals {
			h.stats.removalsRebuilt++
			// A window of removals alone, within the staleness bound, must be
			// followed; this workload's vocabulary is far too small for the
			// dead-term rule to apply.
			if pureRemoval && !predictedRebuild {
				h.stats.pureRemovalRebuilds++
				h.t.Errorf("%s: an index was rebuilt for a plain removal that the log could report", label)
			}
		}
	}
	ftsCheckInvariants(h.t, after, len(table.Rows), label)
	return after
}

// reference builds the same rows in a separate database from scratch.
func (h *fzHarness) reference() (*storage.DB, *storage.Table) {
	return h.referenceOf(h.table())
}

// referenceOf builds the rows of live again in a separate database.
func (h *fzHarness) referenceOf(live *storage.Table) (*storage.DB, *storage.Table) {
	ref := storage.NewTable("fz", live.Cols, false)
	ref.Rows = make([][]any, len(live.Rows))
	for i, row := range live.Rows {
		ref.Rows[i] = slices.Clone(row)
	}
	ref.Version = 1
	db := storage.NewDB()
	if err := db.Put("ref", ref); err != nil {
		h.t.Fatal(err)
	}
	return db, ref
}

func fzQuery(r *rand.Rand, vocab int) string {
	w := func() string {
		w := fzWord(r, vocab)
		w = strings.ReplaceAll(w, "''", "")
		return w
	}
	switch r.Intn(9) {
	case 0:
		return w()
	case 1:
		return w() + " " + w()
	case 2:
		return w() + " OR " + w()
	case 3:
		return w() + " AND " + w()
	case 4:
		return `"` + w() + " " + w() + `"`
	case 5:
		return fzWords[r.Intn(len(fzWords))][:2] + "*"
	case 6:
		return w() + " AND NOT " + w()
	case 7:
		return fmt.Sprintf("w%d*", r.Intn(vocab/3+1))
	default:
		return w() + " " + w() + " " + w()
	}
}

// query draws a search expression: random words, and now and then a phrase made
// of two adjacent words of a row that exists, or a prefix of a word some row once
// had (whether or not any row still has it).
func (h *fzHarness) query(table *storage.Table) string {
	switch h.r.Intn(10) {
	case 0, 1:
		if len(table.Rows) > 0 {
			if text, ok := table.Rows[h.r.Intn(len(table.Rows))][2].(string); ok {
				if words := fzTokens(text); len(words) >= 2 {
					at := h.r.Intn(len(words) - 1)
					return `"` + words[at] + " " + words[at+1] + `"`
				}
			}
		}
	case 2:
		if len(h.seen) > 0 {
			w := []rune(h.seen[h.r.Intn(len(h.seen))])
			return string(w[:min(len(w), 3)]) + "*"
		}
	case 3:
		if h.variant.multi {
			return fmt.Sprintf("n%d", h.r.Intn(5))
		}
	}
	return fzQuery(h.r, h.vocab)
}

func (h *fzHarness) compareQueries(label string, db, refDB *storage.DB, queries int) {
	h.t.Helper()
	ctx := context.Background()
	run := func(db *storage.DB, tenant, sql string) *ResultSet {
		rs, err := Execute(ctx, db, tenant, mustParse(sql))
		if err != nil {
			h.t.Fatalf("%s: %.200s: %v", label, sql, err)
		}
		return rs
	}
	liveTable, err := db.Get("default", "fz")
	if err != nil {
		h.t.Fatal(err)
	}
	colList := `'body'`
	if h.variant.multi {
		colList = `'body', 'note'`
	}
	for q := 0; q < queries; q++ {
		query := h.query(liveTable)
		ftsSQL := fmt.Sprintf(`SELECT id, _fts_score, _fts_rank FROM FTS_SEARCH('fz', %s, %d, %s)`, sqlQuote(query), 5+h.r.Intn(30), colList)
		got, want := run(db, "default", ftsSQL), run(refDB, "ref", ftsSQL)
		if !reflect.DeepEqual(got.Rows, want.Rows) {
			h.t.Fatalf("%s: FTS_SEARCH(%q) differs from a fresh index:\n got  %v\n want %v", label, query, got.Rows, want.Rows)
		}
		if q%5 == 0 {
			vec := fzVec(h.r)
			hybrid := fmt.Sprintf(`SELECT id, _rrf_rank, _rrf_score, _vec_rank, _fts_rank FROM HYBRID_SEARCH('fz', 'embedding', 'body', %s, %s, 6, '{"candidate_k":24,"rrf_k":60,"key_columns":["id"]}') ORDER BY _rrf_rank`, sqlQuote(query), vec)
			got, want := run(db, "default", hybrid), run(refDB, "ref", hybrid)
			if !reflect.DeepEqual(got.Rows, want.Rows) {
				h.t.Fatalf("%s: HYBRID_SEARCH(%q) differs from a fresh index:\n got  %v\n want %v", label, query, got.Rows, want.Rows)
			}
		}
	}
}

// checkpoint compares the maintained index and the searches it serves with a
// build from scratch over the same rows.
func (h *fzHarness) checkpoint(label string, queries int) {
	h.t.Helper()
	h.checks++
	h.checkRows(label)
	maintained := h.followIndex(label)
	refDB, refTable := h.reference()
	_ = getFTSDocCache("ref", refTable, h.cols)
	fresh := refTable.FTSIndexes[h.key]
	ftsCompareCanonical(h.t, maintained, fresh, label)
	h.compareQueries(label, h.db, refDB, queries)
	// The searches above may have moved the index on; it must still be sound.
	ftsCheckInvariants(h.t, h.table().FTSIndexes[h.key], len(h.table().Rows), label+" after queries")
}

func (h *fzHarness) run(ops int) {
	h.checkpoint("initial", 6)
	for i := 0; i < ops; i++ {
		label := fmt.Sprintf("op %d", i)
		switch k := h.r.Intn(40); {
		case k == 0:
			h.transaction()
		case k == 1 && h.disk:
			h.reopen()
		default:
			op := h.randomOp(h.state)
			h.apply(h.db, h.state, op)
		}
		h.noteWords()
		if h.r.Intn(3) == 0 {
			h.followIndex(label)
		}
		if h.r.Intn(12) == 0 {
			h.checkpoint(label, 12)
		}
	}
	h.checkpoint("final", 55)
}

func TestFTSIndexFollowsDeletesAndUpdatesLikeAColdBuild(t *testing.T) {
	seeds := 40
	if testing.Short() {
		seeds = 6
	}
	var followed, rebuilt, removalsFollowed, removalsRebuilt, checks int
	for seed := 1; seed <= seeds; seed++ {
		t.Run(fmt.Sprintf("seed%d", seed), func(t *testing.T) {
			h := newFZ(t, int64(seed), false)
			h.run(30 + h.r.Intn(171))
			followed += h.stats.followed
			rebuilt += h.stats.rebuilt
			removalsFollowed += h.stats.removalsFollowed
			removalsRebuilt += h.stats.removalsRebuilt
			checks += h.checks
		})
	}
	t.Logf("%d checkpoints; index carried forward %d times, rebuilt %d times; of the calls that had to apply removals %d were followed, %d rebuilt",
		checks, followed, rebuilt, removalsFollowed, removalsRebuilt)
	if removalsFollowed < 20 || removalsFollowed < removalsRebuilt/4 {
		t.Fatalf("the incremental removal path was barely exercised: followed %d, rebuilt %d", removalsFollowed, removalsRebuilt)
	}
}

// The same workload against the table shapes an application is likely to have:
// a PRIMARY KEY (point DELETEs reorder rows and use the single-row rollback
// snapshot), a secondary index on the source column, and an FTS index over two
// columns next to the single-column one HYBRID_SEARCH builds.
func TestFTSIndexFollowsOnOtherTableShapes(t *testing.T) {
	seeds := 12
	if testing.Short() {
		seeds = 3
	}
	for name, variant := range map[string]fzVariant{
		"primary key":           {pk: true},
		"primary key and index": {pk: true, srcIndex: true},
		"source index":          {srcIndex: true},
		"two columns":           {multi: true},
		"everything":            {pk: true, srcIndex: true, multi: true},
	} {
		t.Run(name, func(t *testing.T) {
			var removalsFollowed, removalsRebuilt int
			for seed := 201; seed < 201+seeds; seed++ {
				t.Run(fmt.Sprintf("seed%d", seed), func(t *testing.T) {
					h := newFZ(t, int64(seed), false, variant)
					h.run(30 + h.r.Intn(120))
					removalsFollowed += h.stats.removalsFollowed
					removalsRebuilt += h.stats.removalsRebuilt
				})
			}
			t.Logf("removal windows followed %d, rebuilt %d", removalsFollowed, removalsRebuilt)
		})
	}
}

// The same workload with a disk-backed database that is closed and reopened in
// between, so the persisted index is followed after a reload too.
func TestFTSIndexFollowsAcrossReopen(t *testing.T) {
	seeds := 8
	if testing.Short() {
		seeds = 2
	}
	for seed := 101; seed < 101+seeds; seed++ {
		t.Run(fmt.Sprintf("seed%d", seed), func(t *testing.T) {
			h := newFZ(t, int64(seed), true)
			h.run(40 + h.r.Intn(80))
		})
	}
}

// A removal of every kind of position pattern applied through the index
// directly, without SQL: first, last, adjacent, scattered, all, none.
func TestFTSApplyRemovalsPatterns(t *testing.T) {
	rng := rand.New(rand.NewSource(77))
	for round := 0; round < 300; round++ {
		n := 1 + rng.Intn(400)
		table := storage.NewTable("fz", []storage.Column{{Name: "body", Type: storage.TextType}}, false)
		for i := 0; i < n; i++ {
			table.Rows = append(table.Rows, []any{fzText(rng, 60)})
		}
		cols := []int{0}
		index := &storage.FTSIndex{}
		ftsExtendPersistent(table, cols, index)
		var dead []int
		switch rng.Intn(5) {
		case 0:
			dead = []int{0}
		case 1:
			dead = []int{n - 1}
		case 2:
			start := rng.Intn(n)
			for p := start; p < min(n, start+1+rng.Intn(20)); p++ {
				dead = append(dead, p)
			}
		case 3:
			for p := 0; p < n; p++ {
				if rng.Intn(3) == 0 {
					dead = append(dead, p)
				}
			}
		default:
			for p := 0; p < n; p++ {
				dead = append(dead, p)
			}
		}
		if len(dead) == 0 {
			continue
		}
		if !ftsApplyRemovals(index, dead) {
			t.Fatalf("round %d: removal refused", round)
		}
		survivors := slices.Clone(table.Rows)
		for i := len(dead) - 1; i >= 0; i-- {
			survivors = slices.Delete(survivors, dead[i], dead[i]+1)
		}
		index.BuiltRows = len(survivors)
		if index.NumDocs > 0 {
			index.AvgDocLen = index.TotalDocLen / float64(index.NumDocs)
		} else {
			index.AvgDocLen = 0
		}
		cold := storage.NewTable("fz", table.Cols, false)
		cold.Rows = survivors
		fresh := &storage.FTSIndex{}
		ftsExtendPersistent(cold, cols, fresh)
		index.SetVerified(true)
		label := fmt.Sprintf("round %d (%d rows, %d removed)", round, n, len(dead))
		ftsCheckInvariants(t, index, len(survivors), label)
		ftsCompareCanonical(t, index, fresh, label)
	}
}

// An index that is damaged is never followed: a dead document that is missing
// from its posting list makes the removal refuse.
func TestFTSApplyRemovalsRefusesAnInconsistentIndex(t *testing.T) {
	table := storage.NewTable("fz", []storage.Column{{Name: "body", Type: storage.TextType}}, false)
	for i := 0; i < 50; i++ {
		table.Rows = append(table.Rows, []any{fmt.Sprintf("alpha beta w%d", i)})
	}
	index := &storage.FTSIndex{}
	ftsExtendPersistent(table, []int{0}, index)
	index.Postings["alpha"] = slices.DeleteFunc(slices.Clone(index.Postings["alpha"]), func(row int32) bool { return row == 7 })
	index.PostingCounts["alpha"] = index.PostingCounts["alpha"][:len(index.Postings["alpha"])]
	if ftsApplyRemovals(index, []int{7}) {
		t.Fatal("a removal from an inconsistent index was accepted")
	}
	if ftsApplyRemovals(index, []int{50}) || ftsApplyRemovals(index, []int{-1}) {
		t.Fatal("a position outside the index was accepted")
	}
}
