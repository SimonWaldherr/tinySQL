package main

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime/pprof"
	"strings"
	"time"
)

// runUpdProf: how does the first hybrid after a batched touch UPDATE (an
// importer marking the rows of many sources as seen, only a timestamp column)
// scale with the number of updated rows?
func runUpdProf() {
	openMem()
	d := loadCorpus(*nF)
	logf("UP load %d rows: %.1f s", *nF, d.Seconds())
	g := newGen(1001, false)
	sqlx(`SELECT * FROM FTS_WARM('chunks', 'content')`)
	sqlx(`SELECT * FROM VEC_WARM('chunks', 'embedding', 'cosine', 'flat')`)
	g.hybrid("flat", *limitF)
	for _, nsrc := range []int{10, 50, 150, 300, 500, 650, 500} {
		for _, probe := range []string{"hybrid", "fts+vec"} {
			g.hybrid("flat", *limitF)
			st := updMany(nsrc)
			aff := strings.TrimSpace(st[0].aff)
			var line string
			if probe == "hybrid" {
				prof := nsrc == 500 && *profDir != ""
				var f *os.File
				if prof {
					f, _ = os.Create(filepath.Join(*profDir, fmt.Sprintf("cpu-upd500-%d.prof", *nF)))
					pprof.StartCPUProfile(f)
				}
				d1, _ := g.hybrid("flat", *limitF)
				if prof {
					pprof.StopCPUProfile()
					f.Close()
				}
				d2, _ := g.hybrid("flat", *limitF)
				line = fmt.Sprintf("first hybrid %.1f ms, second %.1f ms", msf(d1), msf(d2))
			} else {
				d1, _ := g.fts(*limitF)
				d2, _ := g.vecSearch("flat", *limitF)
				d3, _ := g.hybrid("flat", *limitF)
				line = fmt.Sprintf("FTS first %.1f ms, then VEC %.1f ms, then hybrid %.1f ms", msf(d1), msf(d2), msf(d3))
			}
			logf("UP n=%d UPDATE %d sources (%s, dml %.1f ms) probe=%s: %s", *nF, nsrc, aff, msf(st[0].wall), probe, line)
			time.Sleep(10 * time.Millisecond)
		}
	}
}
