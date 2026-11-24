package main

// convoysync (E9): the import pattern of an application that persists after every
// document, on a DISK/HYBRID database. One writer re-imports a source per
// iteration (INSERT 6 rows, DELETE the old load of that source, then a Sync), four
// searchers issue a HYBRID_SEARCH every 500 ms. An application-level reader/writer
// lock (dbMu: statements take the read lock, the save step takes the write lock
// around db.Sync()) is emulated so the search latency includes the time a search
// waits for a Sync, as it would in such an application.
//
//	-syncmode excl   Sync under the exclusive dbMu (what an application needs when Sync cannot run beside writers)
//	-syncmode rlock  Sync under the shared dbMu (only valid for a tinySQL with the snapshot Sync)
//	-syncmode none   Sync without any outer lock (only valid for a tinySQL with the snapshot Sync)

import (
	"flag"
	"fmt"
	"os"
	"sync"
	"time"

	tinysql "github.com/SimonWaldherr/tinySQL"
)

var (
	syncMode  = flag.String("syncmode", "excl", "convoysync: excl|rlock|none")
	syncEvery = flag.Int("syncevery", 1, "convoysync: Sync after every N writer iterations")
	csDur     = flag.Int("csdur", 90, "convoysync: writer phase seconds")
	csCtrl    = flag.Int("csctrl", 20, "convoysync: control phase seconds (no writer)")
)

var dbMu sync.RWMutex

func syncOnce() (wait, exec time.Duration) {
	var err error
	switch *syncMode {
	case "excl":
		t := time.Now()
		dbMu.Lock()
		wait = time.Since(t)
		t = time.Now()
		err = db.Sync()
		dbMu.Unlock()
		exec = time.Since(t)
	case "rlock":
		t := time.Now()
		dbMu.RLock()
		wait = time.Since(t)
		t = time.Now()
		err = db.Sync()
		dbMu.RUnlock()
		exec = time.Since(t)
	case "none":
		t := time.Now()
		err = db.Sync()
		exec = time.Since(t)
	default:
		panic("syncmode")
	}
	if err != nil {
		panic(err)
	}
	return
}

func runConvoySync() {
	path := *dataDir
	if path == "" {
		panic("convoysync needs -data")
	}
	os.MkdirAll(path, 0o755)
	openDisk(path)
	createSchema()
	d := loadCorpus(*nF)
	logf("E9 load %d rows into %s store: %.1f s", *nF, *storageF, d.Seconds())
	t := time.Now()
	if err := db.Sync(); err != nil {
		panic(err)
	}
	logf("E9 initial Sync: %.1f ms", msf(time.Since(t)))
	sqlx(`SELECT * FROM FTS_WARM('chunks', 'content')`)
	sqlx(`SELECT * FROM VEC_WARM('chunks', 'embedding', 'cosine', 'flat')`)
	g0 := newGen(555, false)
	for i := 0; i < 3; i++ {
		g0.hybrid("flat", *limitF)
	}
	t = time.Now()
	if err := db.Sync(); err != nil {
		panic(err)
	}
	logf("E9 Sync after warm-up: %.1f ms", msf(time.Since(t)))
	memLine("convoysync start")

	phase := func(name string, dur time.Duration, writer bool) {
		logf("\n=== E9 phase %s (%.0fs, writer=%v, syncmode=%s, syncevery=%d) ===", name, dur.Seconds(), writer, *syncMode, *syncEvery)
		var mu sync.Mutex
		var recs []rec
		var ins, del, swait, sexec []float64
		iterations := 0
		var wg sync.WaitGroup
		t0 := time.Now()
		end := t0.Add(dur)
		for s := 0; s < 4; s++ {
			wg.Add(1)
			go func(s int) {
				defer wg.Done()
				g := newGen(int64(9000+s), false)
				time.Sleep(time.Duration(s) * 125 * time.Millisecond)
				for time.Now().Before(end) {
					start := time.Now()
					dbMu.RLock()
					g.hybrid("flat", *limitF)
					dbMu.RUnlock()
					lat := time.Since(start)
					mu.Lock()
					recs = append(recs, rec{start.Sub(t0), lat})
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
					dbMu.RLock()
					_, err := run(st)
					dbMu.RUnlock()
					if err != nil {
						panic(err)
					}
					i1 := msf(time.Since(t1))
					t2 := time.Now()
					stmt, _ := tinysql.ParseSQL(fmt.Sprintf("DELETE FROM chunks WHERE source_id = '%s' AND load_id <> '%s'", srcID(s), load))
					dbMu.RLock()
					_, err = run0(stmt)
					dbMu.RUnlock()
					if err != nil {
						panic(err)
					}
					d2 := msf(time.Since(t2))
					var w, x time.Duration
					if it%*syncEvery == 0 {
						w, x = syncOnce()
					}
					mu.Lock()
					iterations++
					ins = append(ins, i1)
					del = append(del, d2)
					if it%*syncEvery == 0 {
						swait = append(swait, msf(w))
						sexec = append(sexec, msf(x))
					}
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
		logf("E9 n=%d phase=%s searches=%d p50=%.1f p90=%.1f p99=%.1f max=%.1f ms; >1s=%d >5s=%d >25s=%d", *nF, name, len(ls), pct(ls, 50), pct(ls, 90), pct(ls, 99), pct(ls, 100), c1, c5, c25)
		if writer {
			logf("E9 n=%d writer iterations=%d (%.2f/s) INSERT6 ms p50=%.1f p90=%.1f max=%.1f | DELETE ms p50=%.1f p90=%.1f max=%.1f | Sync wait-for-lock ms p50=%.1f max=%.1f | Sync exec ms p50=%.1f max=%.1f (n=%d)",
				*nF, iterations, float64(iterations)/dur.Seconds(), pct(ins, 50), pct(ins, 90), pct(ins, 100), pct(del, 50), pct(del, 90), pct(del, 100), pct(swait, 50), pct(swait, 100), pct(sexec, 50), pct(sexec, 100), len(sexec))
		}
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
	phase("control(no writer)", time.Duration(*csCtrl)*time.Second, false)
	phase("writer 1 src/s + Sync", time.Duration(*csDur)*time.Second, true)
	logf("final %s", rssMB())
	t = time.Now()
	if err := db.Close(); err != nil {
		logf("close err: %v", err)
	}
	logf("E9 Close: %.1f ms", msf(time.Since(t)))
}
