package engine

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// TestMeasureSync prints how long DB.Sync takes, how much it writes and how
// much heap it needs, for the old path (the backend saves the live table, which
// is what DB.Sync did before snapshots) and the snapshot path. It does nothing
// unless TSQL_MEASURE_SYNC=<rows>x<dims> is set, for example 100000x768.
//
// Environment variables (all optional):
//
//	TSQL_MEASURE_SYNC=<rows>x<dims>  switches the test on
//	TSQL_MEASURE_SKIP=on|off         measure only with / without SkipSearchIndexPersistence
//	TSQL_MEASURE_FTS=1               build the FTS image even when it is skipped
//	TSQL_MEASURE_INDEX=1             add a secondary index on id
//	TSQL_MEASURE_BLOCKS=1            use RowBlockFiles
//	TSQL_MEASURE_ROUNDS=<n>          rounds of (live-table, snapshot) writes, default 2
//	TSQL_MEASURE_WORDS=<n>           words per chunk body, default 25
//	TSQL_MEASURE_VOCAB=<n>           vocabulary size, default 5000; setting it makes
//	                                 the word frequencies Zipf distributed
func TestMeasureSync(t *testing.T) {
	spec := os.Getenv("TSQL_MEASURE_SYNC")
	if spec == "" {
		t.Skip("set TSQL_MEASURE_SYNC=<rows>x<dims> to run")
	}
	if v, err := strconv.Atoi(os.Getenv("TSQL_MEASURE_WORDS")); err == nil && v > 0 {
		bigVectorWords = v
	}
	if v, err := strconv.Atoi(os.Getenv("TSQL_MEASURE_VOCAB")); err == nil && v > 1 {
		bigVectorVocab = v
		bigVectorZipf = true
	}
	rounds := 2
	if v, err := strconv.Atoi(os.Getenv("TSQL_MEASURE_ROUNDS")); err == nil && v > 0 {
		rounds = v
	}
	parts := strings.SplitN(spec, "x", 2)
	rows, _ := strconv.Atoi(parts[0])
	dims, _ := strconv.Atoi(parts[1])

	variants := []bool{false, true}
	switch os.Getenv("TSQL_MEASURE_SKIP") {
	case "off":
		variants = []bool{false}
	case "on":
		variants = []bool{true}
	}
	for _, skip := range variants {
		dir := t.TempDir()
		db, err := storage.OpenDB(storage.StorageConfig{
			Mode: storage.ModeHybrid, Path: dir, MaxMemoryBytes: 8 << 30, SkipSearchIndexPersistence: skip, RowBlockFiles: os.Getenv("TSQL_MEASURE_BLOCKS") != "",
		})
		if err != nil {
			t.Fatal(err)
		}
		live := bigVectorTable("docs", rows, dims)
		if err := db.Put("default", live); err != nil {
			t.Fatal(err)
		}
		if !skip || os.Getenv("TSQL_MEASURE_FTS") != "" {
			execSQL(t, db, `SELECT * FROM FTS_WARM('docs', 'body')`)
		}
		if os.Getenv("TSQL_MEASURE_INDEX") != "" {
			execSQL(t, db, `CREATE INDEX idx_id ON docs (id)`)
		}
		path := filepath.Join(dir, "default", "docs.tbl")
		var next atomic.Int64
		next.Store(int64(rows))
		dirty := func() {
			n := next.Add(1)
			execSQL(t, db, fmt.Sprintf(`INSERT INTO docs (id, body, emb) VALUES (%d, 'new chunk', '%s')`, n, vectorLiteral(dims, 0.5)))
		}
		measure := func(label string, f func()) {
			runtime.GC()
			var base runtime.MemStats
			runtime.ReadMemStats(&base)
			var peak atomic.Uint64
			stop := make(chan struct{})
			done := make(chan struct{})
			go func() {
				defer close(done)
				for {
					var m runtime.MemStats
					runtime.ReadMemStats(&m)
					if m.HeapAlloc > peak.Load() {
						peak.Store(m.HeapAlloc)
					}
					select {
					case <-stop:
						return
					case <-time.After(20 * time.Millisecond):
					}
				}
			}()
			started := time.Now()
			f()
			elapsed := time.Since(started)
			close(stop)
			<-done
			info, _ := os.Stat(path)
			t.Logf("skip=%v %-12s wall=%-8v file=%6.1f MB  peak heap=%6.0f MB (+%0.0f MB over idle)  content-lock hold=%v image-copy=%v",
				skip, label, elapsed.Round(time.Millisecond), float64(info.Size())/1e6,
				float64(peak.Load())/1e6, float64(peak.Load()-base.HeapAlloc)/1e6, db.LastSyncSnapshotHold(), db.LastSyncImageCopy())
		}
		for round := 0; round < rounds; round++ {
			dirty()
			measure("live-table", func() {
				if err := db.SyncTable("default", live); err != nil {
					t.Fatal(err)
				}
			})
			dirty()
			measure("snapshot", func() {
				if err := db.Sync(); err != nil {
					t.Fatal(err)
				}
			})
		}
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		ls := time.Now()
		reopened, err := storage.OpenDB(storage.StorageConfig{Mode: storage.ModeHybrid, Path: dir, MaxMemoryBytes: 8 << 30, SkipSearchIndexPersistence: skip, RowBlockFiles: os.Getenv("TSQL_MEASURE_BLOCKS") != ""})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := reopened.Get("default", "docs"); err != nil {
			t.Fatal(err)
		}
		t.Logf("skip=%v load after reopen: %v", skip, time.Since(ls).Round(time.Millisecond))
		_ = reopened.Close()
	}
}
