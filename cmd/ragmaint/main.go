// Command ragmaint measures what an embedding application with a large,
// continuously re-imported RAG table pays in tinySQL around a search. It uses the
// public API only and the statement shapes of an importer-fed "chunks" table.
// Modes: core (E1-E5, E10), convoy (E6), persist (E7), hnsw (E8), convoysync (E9)
// and updprof (UP); see README.md in this directory.
package main

import (
	"context"
	"flag"
	"fmt"
	"io/fs"
	"math"
	"math/rand"
	"os"
	osexec "os/exec"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"runtime/pprof"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	tinysql "github.com/SimonWaldherr/tinySQL"
)

func tinyStatementType[T tinysql.Statement](stmt tinysql.Statement, _ T) T { return stmt.(T) }

func tinySearchTemplate(query string) func(...any) tinysql.Statement {
	template := tinyStatementType(tinysql.MustParseSQL(query), tinysql.SelectStar().Build())
	return func(args ...any) tinysql.Statement {
		stmt := *template
		call := *template.From.TableFunc
		call.Args = slices.Clone(call.Args)
		for i, arg := range args {
			call.Args[i] = tinysql.Val(arg).Build()
		}
		stmt.From.TableFunc = &call
		return &stmt
	}
}

const cols = "source_id, source_kind, source_name, load_id, loaded_at, doc_date, chunk_idx, content, embed_model"

var hybridT = tinySearchTemplate("SELECT " + cols + ", _vec_similarity, _fts_score FROM HYBRID_SEARCH(NULL,NULL,NULL,NULL,NULL,NULL,NULL)")
var vecT = tinySearchTemplate("SELECT " + cols + ", _vec_distance FROM VEC_SEARCH(NULL,NULL,NULL,NULL,NULL,NULL)")
var ftsT = tinySearchTemplate("SELECT source_id, chunk_idx, embed_model, _fts_score FROM FTS_SEARCH(NULL,NULL,NULL,NULL)")

var (
	db  *tinysql.DB
	ctx = context.Background()

	mode     = flag.String("mode", "core", "core|convoy|persist|hnsw|convoysync|updprof")
	nF       = flag.Int("n", 100000, "chunks")
	dim      = flag.Int("dim", 768, "vector dims")
	reps     = flag.Int("reps", 3, "repetitions for E3")
	limitF   = flag.Int("limit", 300, "candidate_k / k")
	gcp      = flag.Int("gc", 50, "GC percent")
	memlimit = flag.Int("memlimit", 10, "soft memory limit GiB")
	dataDir  = flag.String("data", "", "data dir for persist mode")
	storageF = flag.String("storage", "disk", "disk|hybrid")
	profDir  = flag.String("prof", "", "directory for pprof output (empty: none)")
	convDur  = flag.Int("convdur", 120, "convoy writer phase seconds")
	ctrlDur  = flag.Int("ctrldur", 60, "convoy control phase seconds")
	index    = flag.String("index", "flat", "flat|hnsw for core searches")
	rssCap   = flag.Float64("rsscap", 0, "abort above this many GiB")
	skipMut  = flag.String("skip", "", "comma list of substrings: skip mutations whose name contains one")
	onlyMut  = flag.String("only", "", "comma list of substrings: run only mutations whose name contains one")
	skipE3   = flag.Bool("skipe3", false, "skip E3 (core)")
)

func sqlx(q string) *tinysql.ResultSet {
	stmt, err := tinysql.ParseSQL(q)
	if err != nil {
		panic(fmt.Sprintf("parse %.200q: %v", q, err))
	}
	rs, err := tinysql.Execute(ctx, db, "default", stmt)
	if err != nil {
		panic(fmt.Sprintf("exec %.200q: %v", q, err))
	}
	return rs
}

func run(stmt tinysql.Statement) (*tinysql.ResultSet, error) {
	return tinysql.Execute(ctx, db, "default", stmt)
}

func msf(d time.Duration) float64 { return float64(d.Microseconds()) / 1000 }

func logf(format string, a ...any) { fmt.Printf(format+"\n", a...) }

// ---------------------------------------------------------------- generators

var vocab []string
var centers [][]float64
var artSample []string
var sufs = []string{"2rs", "zz", "c3", "p", "x2", "k", "1", "2z", "n", "rs1", "e", "b4", "m", "t5", "q"}

func syl(d int) string {
	const cons = "bcdfghjklmnpqrstvwxz"
	const vow = "aeiou"
	return string([]byte{cons[d/5], vow[d%5]})
}

func initGlobals() {
	vocab = make([]string, 50000)
	for i := range vocab {
		vocab[i] = syl(i/10000) + syl((i/100)%100) + syl(i%100)
	}
	r := rand.New(rand.NewSource(7))
	centers = make([][]float64, 300)
	for i := range centers {
		c := make([]float64, *dim)
		for j := range c {
			c[j] = r.NormFloat64()
		}
		centers[i] = c
	}
}

type gen struct {
	r    *rand.Rand
	z    *rand.Zipf
	keep bool
}

func newGen(seed int64, keep bool) *gen {
	r := rand.New(rand.NewSource(seed))
	return &gen{r: r, z: rand.NewZipf(r, 1.08, 10, uint64(len(vocab)-1)), keep: keep}
}

func artTok(i int) string {
	return fmt.Sprintf("%c%c%d", 'a'+i%26, 'a'+(i/26)%26, i/676)
}

func (g *gen) text(art bool) string {
	nw := 290 + g.r.Intn(61)
	var b strings.Builder
	b.Grow(nw*7 + 64)
	na := 0
	if art {
		na = 3 + g.r.Intn(6)
	}
	for i := 0; i < nw; i++ {
		if i > 0 {
			b.WriteByte(' ')
		}
		if na > 0 && g.r.Intn(nw-i) < na {
			na--
			t := artTok(g.r.Intn(5_000_000))
			if g.keep && g.r.Intn(100) == 0 {
				artSample = append(artSample, t)
			}
			b.WriteString(t)
			b.WriteByte('-')
			b.WriteString(sufs[g.r.Intn(len(sufs))])
		} else {
			b.WriteString(vocab[g.z.Uint64()])
		}
	}
	return b.String()
}

func (g *gen) vec() []float64 {
	c := centers[g.r.Intn(len(centers))]
	v := make([]float64, *dim)
	var norm float64
	for i := range v {
		v[i] = c[i] + 0.35*g.r.NormFloat64()
		norm += v[i] * v[i]
	}
	norm = math.Sqrt(norm)
	for i := range v {
		v[i] /= norm
	}
	return v
}

func (g *gen) queryText() string {
	nt := 4 + g.r.Intn(5)
	seen := map[string]bool{}
	terms := []string{}
	if len(artSample) > 0 && g.r.Intn(100) < 15 {
		t := artSample[g.r.Intn(len(artSample))]
		seen[t] = true
		terms = append(terms, t)
	}
	for len(terms) < nt {
		w := vocab[30+g.r.Intn(3970)]
		if !seen[w] {
			seen[w] = true
			terms = append(terms, w)
		}
	}
	return strings.Join(terms, " OR ")
}

// ---------------------------------------------------------------- data model

type rowT struct {
	id, src, idx int
	load         string
	content      string
	vec          []float64
}

var nextID int
var nextSrc int
var alive []int // source numbers that exist
var ldg *gen    // loader generator

func newSourceRows(g *gen, load string, k int) []rowT {
	src := nextSrc
	nextSrc++
	alive = append(alive, src)
	rows := make([]rowT, k)
	for j := 0; j < k; j++ {
		nextID++
		rows[j] = rowT{id: nextID, src: src, idx: j, load: load, content: g.text(g.r.Intn(4) == 0), vec: g.vec()}
	}
	return rows
}

var kinds = []string{"sharepoint", "mail", "pdf", "wiki", "rss_entry", "artikelwissen", "teams", "web", "docx", "xlsx", "pptx", "ticket"}

func srcID(n int) string { return fmt.Sprintf("src%07d", n) }

func buildInsert(rows []rowT) tinysql.Statement {
	ins := tinysql.InsertInto("chunks").Columns("id", "source_id", "source_kind", "source_name", "load_id", "loaded_at", "content_hash", "doc_date", "chunk_idx", "content", "embedding", "embed_model", "chunk_hash", "embedding_key")
	for _, r := range rows {
		ins.Values(tinysql.Val(r.id), tinysql.Val(srcID(r.src)), tinysql.Val(kinds[r.src%len(kinds)]), tinysql.Val(fmt.Sprintf("doc %d", r.src)), tinysql.Val(r.load), tinysql.Val(int64(1700000000+r.src)), tinysql.Val("h"), tinysql.Val(int64(0)), tinysql.Val(r.idx), tinysql.Val(r.content), tinysql.Val(r.vec), tinysql.Val("nomic"), tinysql.Val(fmt.Sprintf("ch%d", r.id)), tinysql.Val(fmt.Sprintf("ek%d", r.id)))
	}
	return ins.Build()
}

type dmlStat struct {
	wall  time.Duration
	alloc uint64
	aff   string
}

func timed(fn func() (*tinysql.ResultSet, error)) dmlStat {
	var m0, m1 runtime.MemStats
	runtime.ReadMemStats(&m0)
	t := time.Now()
	rs, err := fn()
	d := time.Since(t)
	runtime.ReadMemStats(&m1)
	if err != nil {
		panic(err)
	}
	aff := ""
	if rs != nil && len(rs.Rows) > 0 {
		aff = fmt.Sprint(rs.Rows[0])
	}
	return dmlStat{d, m1.TotalAlloc - m0.TotalAlloc, aff}
}

func timedSQL(q string) dmlStat {
	return timed(func() (*tinysql.ResultSet, error) {
		stmt, err := tinysql.ParseSQL(q)
		if err != nil {
			return nil, err
		}
		return tinysql.Execute(ctx, db, "default", stmt)
	})
}

func timedStmt(s tinysql.Statement) dmlStat {
	return timed(func() (*tinysql.ResultSet, error) { return run(s) })
}

// ingestRows inserts about `total` rows in 16-row statements, the way an importer
// writes the chunks of a document.
func ingestRows(g *gen, total int, load string, collect bool) []dmlStat {
	var stats []dmlStat
	var pend []rowT
	flush := func() {
		if len(pend) == 0 {
			return
		}
		st := buildInsert(pend)
		if collect {
			stats = append(stats, timedStmt(st))
		} else if _, err := run(st); err != nil {
			panic(err)
		}
		pend = pend[:0]
	}
	done := 0
	for done < total {
		k := 3 + g.r.Intn(7)
		for _, r := range newSourceRows(g, load, k) {
			pend = append(pend, r)
			done++
			if len(pend) == 16 {
				flush()
			}
		}
	}
	flush()
	return stats
}

func createSchema() {
	sqlx(`CREATE TABLE chunks (
		id INT, source_id TEXT, source_kind TEXT, source_name TEXT,
		load_id TEXT, loaded_at INT, content_hash TEXT, doc_date INT,
		chunk_idx INT, content TEXT, embedding VECTOR, embed_model TEXT,
		chunk_hash TEXT, embedding_key TEXT
	)`)
	sqlx(`CREATE INDEX IF NOT EXISTS idx_chunks_source_chunk ON chunks(source_id, chunk_idx)`)
}

func loadCorpus(n int) time.Duration {
	ldg = newGen(42, true)
	t := time.Now()
	last := time.Now()
	done := 0
	for done < n {
		chunk := 20000
		if n-done < chunk {
			chunk = n - done
		}
		ingestRows(ldg, chunk, "L1", false)
		done += chunk
		if time.Since(last) > 20*time.Second {
			logf("   ... loaded %d/%d rows (%.1fs)", done, n, time.Since(t).Seconds())
			last = time.Now()
		}
	}
	return time.Since(t)
}

// ---------------------------------------------------------------- searches

func opts(idx string, limit int) string {
	return fmt.Sprintf(`{"candidate_k":%d,"rrf_k":60,"metric":"cosine","index":%q,"auto_or_expand":false,"key_columns":["source_id","chunk_idx","load_id"]}`, limit, idx)
}

func (g *gen) hybrid(idx string, limit int) (time.Duration, int) {
	q, v := g.queryText(), g.vec()
	t := time.Now()
	rs, err := run(hybridT("chunks", "embedding", "content", q, v, limit, opts(idx, limit)))
	d := time.Since(t)
	if err != nil {
		panic(err)
	}
	return d, len(rs.Rows)
}

func (g *gen) fts(limit int) (time.Duration, int) {
	q := g.queryText()
	t := time.Now()
	rs, err := run(ftsT("chunks", q, limit, "content"))
	d := time.Since(t)
	if err != nil {
		panic(err)
	}
	return d, len(rs.Rows)
}

func (g *gen) vecSearch(idx string, limit int) (time.Duration, int) {
	v := g.vec()
	t := time.Now()
	rs, err := run(vecT("chunks", "embedding", v, limit, "cosine", idx))
	d := time.Since(t)
	if err != nil {
		panic(err)
	}
	return d, len(rs.Rows)
}

// ---------------------------------------------------------------- stats helpers

func pct(xs []float64, p float64) float64 {
	if len(xs) == 0 {
		return math.NaN()
	}
	s := slices.Clone(xs)
	sort.Float64s(s)
	i := int(math.Ceil(p/100*float64(len(s)))) - 1
	if i < 0 {
		i = 0
	}
	return s[i]
}

func medRange(xs []float64) string {
	if len(xs) == 0 {
		return "n/a"
	}
	s := slices.Clone(xs)
	sort.Float64s(s)
	return fmt.Sprintf("%.1f [%.1f-%.1f]", pct(xs, 50), s[0], s[len(s)-1])
}

func heapMB() float64 {
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return float64(m.HeapInuse) / 1e6
}

func rssMB() string {
	out, err := osexec.Command("powershell", "-NoProfile", "-Command",
		fmt.Sprintf("$p=Get-Process -Id %d; '{0:F0} {1:F0}' -f ($p.WorkingSet64/1MB),($p.PeakWorkingSet64/1MB)", os.Getpid())).Output()
	if err != nil {
		return "rss n/a"
	}
	f := strings.Fields(string(out))
	if len(f) == 2 {
		return fmt.Sprintf("RSS %s MiB (peak %s MiB)", f[0], f[1])
	}
	return "rss n/a"
}

func memLine(label string) {
	logf("   [mem %s: heap in use %.0f MB; %s]", label, heapMB(), rssMB())
}

func writeHeapProf(name string) {
	if *profDir == "" {
		return
	}
	runtime.GC()
	f, err := os.Create(filepath.Join(*profDir, name))
	if err != nil {
		logf("heap prof: %v", err)
		return
	}
	defer f.Close()
	pprof.Lookup("heap").WriteTo(f, 0)
}

// ---------------------------------------------------------------- mutations

func pickAlive(k int, remove bool) []int {
	out := make([]int, 0, k)
	for i := 0; i < k && len(alive) > 0; i++ {
		j := ldg.r.Intn(len(alive))
		out = append(out, alive[j])
		if remove {
			alive[j] = alive[len(alive)-1]
			alive = alive[:len(alive)-1]
		}
	}
	return out
}

func inList(srcs []int) string {
	var b strings.Builder
	for i, s := range srcs {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString("'" + srcID(s) + "'")
	}
	return b.String()
}

var touchSeq int64 = 1900000000

type mutation struct {
	name string
	fn   func() []dmlStat
}

func insertOne(k int) dmlStat {
	return timedStmt(buildInsert(newSourceRows(ldg, "L2", k)))
}

func delOne() dmlStat {
	s := pickAlive(1, true)[0]
	return timedSQL(fmt.Sprintf("DELETE FROM chunks WHERE source_id = '%s' AND load_id <> 'keep'", srcID(s)))
}

func updMany(nsrc int) []dmlStat {
	srcs := pickAlive(nsrc, false)
	touchSeq++
	return []dmlStat{timedSQL(fmt.Sprintf("UPDATE chunks SET loaded_at = %d WHERE source_id IN (%s) AND COALESCE(loaded_at, 0) < %d", touchSeq, inList(srcs), touchSeq))}
}

func mutations() []mutation {
	return []mutation{
		{"INSERT 6 rows", func() []dmlStat { return []dmlStat{insertOne(6)} }},
		{"INSERT 1000 rows", func() []dmlStat { return ingestRows(ldg, 1000, "L2", true) }},
		{"UPDATE 6 rows", func() []dmlStat { return updMany(1) }},
		{"UPDATE ~3000 rows (500 src)", func() []dmlStat { return updMany(500) }},
		{"UPDATE ~6000 rows (1000 src)", func() []dmlStat { return updMany(1000) }},
		{"DELETE 1 source", func() []dmlStat { return []dmlStat{delOne()} }},
		{"DELETE 100 sources IN", func() []dmlStat {
			srcs := pickAlive(100, true)
			return []dmlStat{timedSQL("DELETE FROM chunks WHERE source_id IN (" + inList(srcs) + ")")}
		}},
		{"DELETE 1 + INSERT 1 (replace)", func() []dmlStat { return []dmlStat{delOne(), insertOne(6)} }},
	}
}

type e3sample struct {
	warm, dmlSum, dmlMax, dmlMean, dmlAllocMB, first, second, third float64
	nstmt                                                           int
	aff                                                             string
}

func sumStats(st []dmlStat) (sum, max, mean, allocMB float64, aff string) {
	var a uint64
	for _, s := range st {
		w := msf(s.wall)
		sum += w
		if w > max {
			max = w
		}
		a += s.alloc
		if aff == "" || len(st) <= 2 {
			aff += s.aff + " "
		}
	}
	mean = sum / float64(len(st))
	allocMB = float64(a) / 1e6
	return
}

func e3(g *gen) {
	logf("\n=== E3/E4: first search after mutation (index=%s, limit=%d, reps=%d) ===", *index, *limitF, *reps)
	idx := *index
	res := map[string][]e3sample{}
	var order []string
	muts := mutations()
	for rep := 1; rep <= *reps; rep++ {
		for _, m := range muts {
			if !mutSelected(m.name) {
				continue
			}
			for _, probe := range []string{"hybrid", "fts+vec"} {
				// re-warm and baseline
				g.hybrid(idx, *limitF)
				var w []float64
				for i := 0; i < 3; i++ {
					d, _ := g.hybrid(idx, *limitF)
					w = append(w, msf(d))
				}
				warm := pct(w, 50)
				st := m.fn()
				sum, max, mean, alloc, aff := sumStats(st)
				s := e3sample{warm: warm, dmlSum: sum, dmlMax: max, dmlMean: mean, dmlAllocMB: alloc, nstmt: len(st), aff: aff}
				if probe == "hybrid" {
					d1, _ := g.hybrid(idx, *limitF)
					d2, _ := g.hybrid(idx, *limitF)
					s.first, s.second = msf(d1), msf(d2)
				} else {
					d1, _ := g.fts(*limitF)
					d2, _ := g.vecSearch(idx, *limitF)
					d3, _ := g.hybrid(idx, *limitF)
					s.first, s.second, s.third = msf(d1), msf(d2), msf(d3)
				}
				key := m.name + " | " + probe
				if _, ok := res[key]; !ok {
					order = append(order, key)
				}
				res[key] = append(res[key], s)
				logf("E3 rep=%d %-32s probe=%-8s warm=%7.1f dml_sum=%8.1f (n=%d max=%7.1f) alloc=%8.1fMB first=%9.1f second=%9.1f third=%9.1f aff=%s", rep, m.name, probe, s.warm, s.dmlSum, s.nstmt, s.dmlMax, s.dmlAllocMB, s.first, s.second, s.third, strings.TrimSpace(s.aff))
			}
		}
	}
	logf("\n--- E3/E4 summary (median [min-max] ms; n=%d rows at start approx) ---", *nF)
	col := func(ss []e3sample, f func(e3sample) float64) string {
		var xs []float64
		for _, s := range ss {
			xs = append(xs, f(s))
		}
		return medRange(xs)
	}
	for _, key := range order {
		ss := res[key]
		logf("SUM3 n=%d | %-48s | warm %s | first %s | second %s | third %s | dml_sum %s | dml_stmt_mean %s | dml_stmt_max %s | dml_alloc_MB %s | stmts=%d",
			*nF, key,
			col(ss, func(s e3sample) float64 { return s.warm }),
			col(ss, func(s e3sample) float64 { return s.first }),
			col(ss, func(s e3sample) float64 { return s.second }),
			col(ss, func(s e3sample) float64 { return s.third }),
			col(ss, func(s e3sample) float64 { return s.dmlSum }),
			col(ss, func(s e3sample) float64 { return s.dmlMean }),
			col(ss, func(s e3sample) float64 { return s.dmlMax }),
			col(ss, func(s e3sample) float64 { return s.dmlAllocMB }),
			ss[0].nstmt)
	}
}

// ---------------------------------------------------------------- core

func openMem() {
	db = tinysql.NewDB()
	createSchema()
}

func rows() int {
	rs := sqlx("SELECT COUNT(*) AS c FROM chunks")
	return int(toInt(rs.Rows[0]["c"]))
}

func toInt(v any) int64 {
	switch x := v.(type) {
	case int:
		return int64(x)
	case int64:
		return x
	case float64:
		return int64(x)
	}
	return -1
}

// runHelperProbes times the O(N) helper statements an application typically
// issues around a search (distinct embedding models, row count, source list and
// kinds refresh), each three times on the loaded corpus. They are plain SQL, so
// every variant runs the same text; the point is to show how much of a search's
// wall time such statements can add.
func runHelperProbes() {
	logf("\n=== E10 helper statements (3 runs each, GC before each) ===")
	probes := []struct{ name, q string }{
		{"DISTINCT embed_model", "SELECT DISTINCT embed_model FROM chunks"},
		{"COUNT(*)", "SELECT COUNT(*) AS c FROM chunks"},
		{"DISTINCT source_kind", "SELECT DISTINCT source_kind FROM chunks"},
		{"listSources GROUP BY 6 cols", `SELECT source_id, source_kind, source_name, load_id, loaded_at, doc_date, COUNT(*) AS cnt
	      FROM chunks GROUP BY source_id, source_kind, source_name, load_id, loaded_at, doc_date
	      ORDER BY loaded_at DESC`},
	}
	for _, p := range probes {
		var ms []float64
		n := 0
		for i := 0; i < 3; i++ {
			runtime.GC()
			t := time.Now()
			rs := sqlx(p.q)
			ms = append(ms, msf(time.Since(t)))
			n = len(rs.Rows)
		}
		logf("E10 n=%d %-28s rows=%d ms=%.1f / %.1f / %.1f", *nF, p.name, n, ms[0], ms[1], ms[2])
	}
}

func runCore() {
	openMem()
	d := loadCorpus(*nF)
	logf("E1 load %d rows (%d sources, 16-row INSERT batches, single goroutine): %.1f s (%.1f us/row)", *nF, nextSrc, d.Seconds(), float64(d.Microseconds())/float64(*nF))
	logf("   COUNT(*)=%d", rows())
	memLine("after load")
	g := newGen(1001, false)
	t := time.Now()
	sqlx(`SELECT * FROM FTS_WARM('chunks', 'content')`)
	logf("E1 FTS_WARM: %.1f ms", msf(time.Since(t)))
	memLine("after FTS_WARM")
	t = time.Now()
	sqlx(fmt.Sprintf(`SELECT * FROM VEC_WARM('chunks', 'embedding', 'cosine', '%s')`, *index))
	logf("E1 VEC_WARM(%s): %.1f ms", *index, msf(time.Since(t)))
	memLine("after VEC_WARM")
	writeHeapProf(fmt.Sprintf("heap-after-load-%d.prof", *nF))

	// E2
	logf("\n=== E2 warm queries (index=%s) ===", *index)
	g.hybrid(*index, 300)
	for _, lim := range []int{300, 1000} {
		var hs, fs, vs []float64
		var rowsGot int
		for i := 0; i < 30; i++ {
			d, r := g.hybrid(*index, lim)
			hs = append(hs, msf(d))
			rowsGot = r
		}
		for i := 0; i < 30; i++ {
			d, _ := g.fts(lim)
			fs = append(fs, msf(d))
			d2, _ := g.vecSearch(*index, lim)
			vs = append(vs, msf(d2))
		}
		logf("E2 n=%d limit=%d HYBRID p50=%.1f p95=%.1f max=%.1f (rows=%d) | FTS_SEARCH p50=%.1f p95=%.1f | VEC_SEARCH(%s) p50=%.1f p95=%.1f",
			*nF, lim, pct(hs, 50), pct(hs, 95), pct(hs, 100), rowsGot, pct(fs, 50), pct(fs, 95), *index, pct(vs, 50), pct(vs, 95))
	}

	runHelperProbes()

	if !*skipE3 {
		e3(g)
	}

	// E5 profile
	if *profDir != "" {
		logf("\n=== E5 profile: first hybrid after DELETE of 1 source ===")
		g.hybrid(*index, *limitF)
		st := delOne()
		logf("E5 DML delete: %.1f ms aff=%s", msf(st.wall), st.aff)
		f, err := os.Create(filepath.Join(*profDir, fmt.Sprintf("cpu-flat-delete-%d.prof", *nF)))
		if err != nil {
			panic(err)
		}
		pprof.StartCPUProfile(f)
		d1, _ := g.hybrid(*index, *limitF)
		pprof.StopCPUProfile()
		f.Close()
		logf("E5 first hybrid after DELETE (profiled): %.1f ms", msf(d1))
		memLine("after rebuild")
		writeHeapProf(fmt.Sprintf("heap-after-rebuild-%d.prof", *nF))
	}
	logf("final COUNT(*)=%d; %s", rows(), rssMB())
}

// ---------------------------------------------------------------- convoy (E6)

type rec struct {
	start time.Duration
	lat   time.Duration
}

func runConvoy() {
	openMem()
	d := loadCorpus(*nF)
	logf("E6 load %d rows: %.1f s", *nF, d.Seconds())
	sqlx(`SELECT * FROM FTS_WARM('chunks', 'content')`)
	sqlx(`SELECT * FROM VEC_WARM('chunks', 'embedding', 'cosine', 'flat')`)
	g0 := newGen(555, false)
	for i := 0; i < 3; i++ {
		g0.hybrid("flat", *limitF)
	}
	memLine("convoy start")

	phase := func(name string, dur time.Duration, writer bool) {
		logf("\n=== E6 phase %s (%.0fs, writer=%v) ===", name, dur.Seconds(), writer)
		var mu sync.Mutex
		var recs []rec
		var ins, del []float64
		var wg sync.WaitGroup
		t0 := time.Now()
		end := t0.Add(dur)
		for s := 0; s < 4; s++ {
			wg.Add(1)
			go func(s int) {
				defer wg.Done()
				g := newGen(int64(9000+s), false)
				// stagger starts
				time.Sleep(time.Duration(s) * 125 * time.Millisecond)
				for time.Now().Before(end) {
					start := time.Now()
					d, _ := g.hybrid("flat", *limitF)
					mu.Lock()
					recs = append(recs, rec{start.Sub(t0), d})
					mu.Unlock()
					next := start.Add(500 * time.Millisecond)
					if now := time.Now(); next.Before(now) {
						next = now
					}
					time.Sleep(time.Until(next))
				}
			}(s)
		}
		if writer {
			wg.Add(1)
			go func() {
				defer wg.Done()
				g := newGen(777, false)
				it := 0
				for time.Now().Before(end) {
					start := time.Now()
					it++
					s := alive[g.r.Intn(len(alive))]
					load := fmt.Sprintf("R%d", it)
					nextID++
					var rs []rowT
					for j := 0; j < 6; j++ {
						nextID++
						rs = append(rs, rowT{id: nextID, src: s, idx: j, load: load, content: g.text(g.r.Intn(4) == 0), vec: g.vec()})
					}
					st := buildInsert(rs)
					t1 := time.Now()
					if _, err := run(st); err != nil {
						panic(err)
					}
					i1 := msf(time.Since(t1))
					t2 := time.Now()
					stmt, _ := tinysql.ParseSQL(fmt.Sprintf("DELETE FROM chunks WHERE source_id = '%s' AND load_id <> '%s'", srcID(s), load))
					rs2, err := run0(stmt)
					if err != nil {
						panic(err)
					}
					d2 := msf(time.Since(t2))
					_ = rs2
					mu.Lock()
					ins = append(ins, i1)
					del = append(del, d2)
					mu.Unlock()
					if next := start.Add(time.Second); next.After(time.Now()) {
						time.Sleep(time.Until(next))
					}
				}
			}()
		}
		wg.Wait()
		var ls []float64
		var c1, c5, c25 int
		for _, r := range recs {
			l := msf(r.lat)
			ls = append(ls, l)
			if l > 1000 {
				c1++
			}
			if l > 5000 {
				c5++
			}
			if l > 25000 {
				c25++
			}
		}
		logf("E6 n=%d phase=%s searches=%d p50=%.1f p90=%.1f p99=%.1f max=%.1f ms; >1s=%d >5s=%d >25s=%d", *nF, name, len(ls), pct(ls, 50), pct(ls, 90), pct(ls, 99), pct(ls, 100), c1, c5, c25)
		if writer {
			logf("E6 n=%d writer iterations=%d INSERT6 ms p50=%.1f p90=%.1f max=%.1f | DELETE ms p50=%.1f p90=%.1f max=%.1f", *nF, len(ins), pct(ins, 50), pct(ins, 90), pct(ins, 100), pct(del, 50), pct(del, 90), pct(del, 100))
		}
		// timeline per 10 s
		for w := 0; w*10 < int(dur.Seconds())+60; w++ {
			var xs []float64
			for _, r := range recs {
				if int(r.start.Seconds())/10 == w {
					xs = append(xs, msf(r.lat))
				}
			}
			if len(xs) > 0 {
				logf("   t=%3d-%3ds searches_started=%3d max=%9.1f median=%9.1f", w*10, w*10+10, len(xs), pct(xs, 100), pct(xs, 50))
			}
		}
	}
	phase("control(no writer)", time.Duration(*ctrlDur)*time.Second, false)
	phase("writer 1 src/s", time.Duration(*convDur)*time.Second, true)
	logf("final %s", rssMB())
}

func run0(stmt tinysql.Statement) (*tinysql.ResultSet, error) {
	return tinysql.Execute(ctx, db, "default", stmt)
}

// ---------------------------------------------------------------- persist (E7)

type fi struct {
	size int64
	mt   time.Time
}

func snapDir(p string) map[string]fi {
	m := map[string]fi{}
	filepath.WalkDir(p, func(path string, d fs.DirEntry, err error) error {
		if err == nil && !d.IsDir() {
			if info, e := d.Info(); e == nil {
				m[path] = fi{info.Size(), info.ModTime()}
			}
		}
		return nil
	})
	return m
}

func total(m map[string]fi) (t int64) {
	for _, v := range m {
		t += v.size
	}
	return
}

func written(before, after map[string]fi) (w int64, files []string) {
	for k, v := range after {
		b, ok := before[k]
		if !ok || b.size != v.size || !b.mt.Equal(v.mt) {
			w += v.size
			files = append(files, fmt.Sprintf("%s=%.1fMB", filepath.Base(k), float64(v.size)/1e6))
		}
	}
	sort.Strings(files)
	return
}

func openDisk(path string) {
	cfg := tinysql.StorageConfig{Mode: tinysql.ModeDisk, Path: path}
	if *storageF == "hybrid" {
		cfg = tinysql.StorageConfig{Mode: tinysql.ModeHybrid, Path: path, MaxMemoryBytes: 6 << 30}
	}
	applyStorageOpts(&cfg)
	var err error
	db, err = tinysql.OpenDB(cfg)
	if err != nil {
		panic(err)
	}
}

func runPersist() {
	path := *dataDir
	os.MkdirAll(path, 0o755)
	logf("E7 storage=%s path=%s n=%d", *storageF, path, *nF)
	openDisk(path)
	createSchema()
	d := loadCorpus(*nF)
	logf("E7 load %d rows into %s store: %.1f s", *nF, *storageF, d.Seconds())
	snap := snapDir(path)
	logf("E7 dir size right after load (before first Sync): %.1f MB, files=%d", float64(total(snap))/1e6, len(snap))

	doSync := func(label string) {
		before := snapDir(path)
		t := time.Now()
		if err := db.Sync(); err != nil {
			panic(err)
		}
		d := time.Since(t)
		after := snapDir(path)
		w, files := written(before, after)
		logf("E7 Sync[%s]: %.1f ms; dir %.1f -> %.1f MB; bytes rewritten (files changed) %.1f MB; changed: %v", label, msf(d), float64(total(before))/1e6, float64(total(after))/1e6, float64(w)/1e6, files)
	}
	doSync("initial full")
	doSync("again, nothing changed")
	g := newGen(1001, false)
	t := time.Now()
	sqlx(`SELECT * FROM FTS_WARM('chunks', 'content')`)
	logf("E7 FTS_WARM: %.1f ms", msf(time.Since(t)))
	t = time.Now()
	sqlx(`SELECT * FROM VEC_WARM('chunks', 'embedding', 'cosine', 'flat')`)
	logf("E7 VEC_WARM flat: %.1f ms", msf(time.Since(t)))
	doSync("after warm (persist FTS/vector sidecars?)")
	g.hybrid("flat", *limitF)

	for rep := 1; rep <= *reps; rep++ {
		for _, m := range mutations() {
			switch m.name {
			case "INSERT 6 rows", "UPDATE 6 rows", "DELETE 1 source", "INSERT 1000 rows":
			default:
				continue
			}
			g.hybrid("flat", *limitF)
			st := m.fn()
			sum, _, _, alloc, aff := sumStats(st)
			logf("E7 rep=%d mutation %-26s dml=%.1f ms alloc=%.1f MB aff=%s", rep, m.name, sum, alloc, strings.TrimSpace(aff))
			doSync("after " + m.name)
			d1, _ := g.hybrid("flat", *limitF)
			logf("E7   first hybrid after: %.1f ms", msf(d1))
			doSync("after first search following " + m.name)
		}
	}
	memLine("before close")
	t = time.Now()
	if err := db.Close(); err != nil {
		logf("close err: %v", err)
	}
	logf("E7 Close: %.1f ms", msf(time.Since(t)))
	db = nil
	debug.FreeOSMemory()
	snap = snapDir(path)
	logf("E7 final dir size %.1f MB files=%d", float64(total(snap))/1e6, len(snap))
	var names []string
	for k, v := range snap {
		names = append(names, fmt.Sprintf("%s=%.1fMB", strings.TrimPrefix(k, path), float64(v.size)/1e6))
	}
	sort.Strings(names)
	logf("E7 files: %v", names)

	t = time.Now()
	openDisk(path)
	logf("E7 reopen OpenDB: %.1f ms", msf(time.Since(t)))
	t = time.Now()
	rs := sqlx("SELECT COUNT(*) AS c FROM chunks")
	logf("E7 first COUNT(*)=%v after reopen: %.1f ms (lazy load)", rs.Rows[0]["c"], msf(time.Since(t)))
	memLine("after reopen+count")
	t = time.Now()
	sqlx(`SELECT * FROM FTS_WARM('chunks', 'content')`)
	logf("E7 FTS_WARM after reopen: %.1f ms", msf(time.Since(t)))
	t = time.Now()
	sqlx(`SELECT * FROM VEC_WARM('chunks', 'embedding', 'cosine', 'flat')`)
	logf("E7 VEC_WARM flat after reopen: %.1f ms", msf(time.Since(t)))
	d1, _ := g.hybrid("flat", *limitF)
	d2, _ := g.hybrid("flat", *limitF)
	logf("E7 hybrid after reopen: first %.1f ms, second %.1f ms", msf(d1), msf(d2))
	memLine("end")
}

// ---------------------------------------------------------------- hnsw (E8)

func runHNSW() {
	openMem()
	d := loadCorpus(*nF)
	logf("E8 load %d rows: %.1f s", *nF, d.Seconds())
	g := newGen(1001, false)
	sqlx(`SELECT * FROM FTS_WARM('chunks', 'content')`)
	h0 := heapMB()
	logf("E8 heap before graph: %.0f MB", h0)
	cctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	stmt, _ := tinysql.ParseSQL(`SELECT * FROM VEC_WARM('chunks', 'embedding', 'cosine', 'hnsw')`)
	t := time.Now()
	_, err := tinysql.Execute(cctx, db, "default", stmt)
	bd := time.Since(t)
	if err != nil {
		logf("E8 VEC_WARM hnsw ERROR: %v", err)
		return
	}
	logf("E8 n=%d VEC_WARM hnsw cold build: %.1f ms = %.4f ms/row", *nF, msf(bd), msf(bd)/float64(*nF))
	h1 := heapMB()
	logf("E8 heap after graph: %.0f MB (delta %.0f MB = %.1f KB/row); %s", h1, h1-h0, (h1-h0)*1000/float64(*nF), rssMB())
	var w []float64
	for i := 0; i < 10; i++ {
		d, _ := g.hybrid("hnsw", *limitF)
		w = append(w, msf(d))
	}
	logf("E8 n=%d warm hybrid(hnsw) p50=%.1f p95=%.1f", *nF, pct(w, 50), pct(w, 95))
	for _, m := range mutations() {
		switch m.name {
		case "INSERT 6 rows", "UPDATE 6 rows", "DELETE 1 source", "INSERT 1000 rows":
		default:
			continue
		}
		g.hybrid("hnsw", *limitF)
		st := m.fn()
		sum, _, _, _, aff := sumStats(st)
		d1, _ := g.hybrid("hnsw", *limitF)
		d2, _ := g.hybrid("hnsw", *limitF)
		logf("E8 n=%d %-20s dml=%.1f ms aff=%s | first hybrid(hnsw)=%.1f ms second=%.1f ms", *nF, m.name, sum, strings.TrimSpace(aff), msf(d1), msf(d2))
	}
	logf("final %s", rssMB())
}

func main() {
	flag.Parse()
	debug.SetGCPercent(*gcp)
	debug.SetMemoryLimit(int64(*memlimit) << 30)
	logf("ragmaint: mode=%s n=%d dim=%d GOMAXPROCS=%d NumCPU=%d go=%s gc=%d memlimit=%dGiB", *mode, *nF, *dim, runtime.GOMAXPROCS(0), runtime.NumCPU(), runtime.Version(), *gcp, *memlimit)
	watchdog(*rssCap)
	initGlobals()
	switch *mode {
	case "core":
		runCore()
	case "convoy":
		runConvoy()
	case "persist":
		runPersist()
	case "updprof":
		runUpdProf()
	case "hnsw":
		runHNSW()
	case "convoysync":
		runConvoySync()
	default:
		panic("mode")
	}
	os.Exit(0)
}
