package engine

import (
	"context"
	"math"
	"reflect"
	"sync"
	"testing"
	"time"
)

// Signalling from Done gives tests a point inside the scoring loop without a
// global hook in the vector search path.
type vectorScanContext struct {
	context.Context
	started chan struct{}
	resume  <-chan struct{}
	once    sync.Once
}

func (ctx *vectorScanContext) Done() <-chan struct{} {
	ctx.once.Do(func() {
		close(ctx.started)
		if ctx.resume != nil {
			<-ctx.resume
		}
	})
	return ctx.Context.Done()
}

func TestVecFlatRankingAllowsConcurrentWrites(t *testing.T) {
	for _, test := range []struct {
		name, metric string
		fallback     bool
		writes       []string
	}{
		{name: "metadata", metric: "l2", writes: []string{`UPDATE vec_docs SET id = 1000 WHERE id = 0`}},
		{name: "append", metric: "l2", writes: []string{`INSERT INTO vec_docs VALUES (1000, '[1,0,0,0]')`}},
		{name: "update vector", metric: "l2", writes: []string{`UPDATE vec_docs SET embedding = '[1,0,0,0]' WHERE id = 2`}},
		{name: "delete", metric: "l2", writes: []string{`DELETE FROM vec_docs WHERE id = 0`}},
		{name: "mixed", metric: "cosine", writes: []string{
			`UPDATE vec_docs SET embedding = '[1,0,0,0]' WHERE id = 2`,
			`DELETE FROM vec_docs WHERE id < 2`,
			`UPDATE vec_docs SET embedding = NULL WHERE id = 3`,
			`INSERT INTO vec_docs VALUES (1000, '[1,0,0,0]'), (1001, '[1,0,0,0]')`,
		}},
		{name: "reserve exhausted", metric: "l2", fallback: true, writes: []string{`DELETE FROM vec_docs WHERE id < 40`}},
	} {
		t.Run(test.name, func(t *testing.T) {
			const tenant = "default"
			db := makeVecSearchBenchmarkTable(96, 4)
			t.Cleanup(func() {
				purgeVectorCachesFor(tenant, "vec_docs")
				_ = db.Close()
			})
			args := vecSearchArgs{tableName: "vec_docs", colName: "embedding", queryVec: []float64{1, 0, 0, 0}, metric: test.metric, indexMode: vecIndexFlat, k: 4}
			needNorm := vecDistanceFuncNeedsNorm(args.metric)
			queryNorm := vectorL2Norm(args.queryVec)
			table, col, err := vecSearchSource(db, tenant, args)
			if err != nil {
				t.Fatal(err)
			}
			cache := getVecColumnCache(tenant, table, col, needNorm)
			resume := make(chan struct{})
			resumeScan := sync.OnceFunc(func() { close(resume) })
			defer resumeScan()
			ctx := &vectorScanContext{Context: t.Context(), started: make(chan struct{}), resume: resume}
			type result struct {
				hits []vecScoredRow
				err  error
			}
			read := make(chan result, 1)
			go func() {
				db.LockContentForRead()
				defer db.UnlockContentForRead()
				_, hits, err := rankVecSearchAlongsideWrites(ctx, ExecEnv{db: db, ctx: ctx}, tenant, args, queryNorm, table, col, cache)
				read <- result{hits, err}
			}()
			select {
			case <-ctx.started:
			case <-time.After(5 * time.Second):
				t.Fatal("vector scan did not start")
			}
			write := make(chan error, 1)
			go func() {
				for _, sql := range test.writes {
					if _, err := Execute(t.Context(), db, tenant, mustParse(sql)); err != nil {
						write <- err
						return
					}
				}
				write <- nil
			}()
			select {
			case err := <-write:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("a writer could not finish while the scan was paused")
			}
			resumeScan()
			got := <-read
			current, col, err := vecSearchSource(db, tenant, args)
			if err != nil {
				t.Fatal(err)
			}
			key := vecSearchColumnCacheKey{source: current, tenant: tenant, table: current.Name, colIdx: col}
			if _, refreshed := currentVecColumnCache(key, needNorm); refreshed != test.fallback {
				t.Fatalf("cache refreshed during ranking = %v, want %v", refreshed, test.fallback)
			}
			coldCache := buildVecColumnCache(current, col, needNorm)
			want, err := vecSearchTopKWithIndex(t.Context(), tenant, current, col, args, queryNorm, coldCache)
			if got.err != nil || err != nil || !reflect.DeepEqual(got.hits, want) {
				t.Fatalf("concurrent ranking = %v/%v, want %v/%v", got.hits, got.err, want, err)
			}
		})
	}
}

// A warm read and one small write compete for the same table. Report the
// write latency separately from the time to finish both operations.
func BenchmarkVecSearchWithConcurrentAppend(b *testing.B) {
	benchmarkVecSearchConcurrentWrite(b, false)
}

func BenchmarkVecSearchWithConcurrentEmbeddingUpdate(b *testing.B) {
	benchmarkVecSearchConcurrentWrite(b, true)
}

func benchmarkVecSearchConcurrentWrite(b *testing.B, update bool) {
	const rows, dims, tenant = 32768, 768, "default"
	db := makeVecSearchBenchmarkTable(rows, dims)
	b.Cleanup(func() {
		purgeVectorCachesFor(tenant, "vec_docs")
		_ = db.Close()
	})
	table, err := db.Get(tenant, "vec_docs")
	if err != nil {
		b.Fatal(err)
	}
	queryVector := make([]float64, dims)
	for i := range queryVector {
		queryVector[i] = math.Cos(float64(i))
	}
	vector := retrievalVecLiteral(queryVector)
	search := mustParse(`SELECT id, _vec_distance FROM VEC_SEARCH('vec_docs', 'embedding', '` + vector + `', 20, 'cosine')`)
	writes := []Statement{mustParse(`INSERT INTO vec_docs VALUES (-1, '` + vector + `')`)}
	if update {
		if _, err := Execute(b.Context(), db, tenant, mustParse(`CREATE INDEX vec_docs_id ON vec_docs(id)`)); err != nil {
			b.Fatal(err)
		}
		old := retrievalVecLiteral(table.Rows[2000][1].([]float64))
		writes = []Statement{
			mustParse(`UPDATE vec_docs SET embedding = '` + vector + `' WHERE id = 2000`),
			mustParse(`UPDATE vec_docs SET embedding = '` + old + `' WHERE id = 2000`),
		}
	}
	getVecColumnCache(tenant, table, 1, true)
	var writeTime time.Duration
	iteration := 0
	b.ReportAllocs()
	for b.Loop() {
		resume := make(chan struct{})
		ctx := &vectorScanContext{Context: b.Context(), started: make(chan struct{}), resume: resume}
		read := make(chan error, 1)
		go func() {
			_, err := Execute(ctx, db, tenant, search)
			read <- err
		}()
		select {
		case <-ctx.started:
		case err := <-read:
			b.Fatalf("read finished before scan: %v", err)
		}
		type writeResult struct {
			elapsed time.Duration
			err     error
		}
		writing := make(chan struct{})
		write := make(chan writeResult, 1)
		statement := writes[iteration%len(writes)]
		iteration++
		go func() {
			started := time.Now()
			close(writing)
			_, err := Execute(b.Context(), db, tenant, statement)
			write <- writeResult{time.Since(started), err}
		}()
		<-writing
		close(resume)
		finished := <-write
		writeTime += finished.elapsed
		if err := <-read; err != nil || finished.err != nil {
			b.Fatalf("read: %v, write: %v", err, finished.err)
		}
		b.StopTimer()
		getVecColumnCache(tenant, table, 1, true)
		b.StartTimer()
	}
	b.ReportMetric(float64(writeTime.Nanoseconds())/float64(b.N), "write-ns/op")
}
