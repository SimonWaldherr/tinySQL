package engine

import (
	"runtime"
	"slices"
	"sync"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// A cold FTS build tokenizes every searched cell of the table, which made the
// first hybrid-retrieval query after startup (or after any UPDATE/DELETE)
// the slowest one by far. ftsBuildFreshParallel splits that work by row
// range. Below ftsParallelBuildMinRows the sequential build in
// ftsExtendPersistent finishes before extra workers would pay for
// themselves.
const (
	ftsParallelBuildMinRows   = 2048
	ftsParallelBuildChunkRows = 1024
)

func ftsBuildWorkerCount(rows int) int {
	if rows < ftsParallelBuildMinRows {
		return 1
	}
	workers := runtime.GOMAXPROCS(0)
	if maxByRows := rows / ftsParallelBuildChunkRows; workers > maxByRows {
		workers = maxByRows
	}
	if workers < 2 {
		return 1
	}
	return workers
}

// ftsBuildChunk is one worker's share of a parallel build: rows [lo, hi)
// tokenized against a chunk-local dictionary.
type ftsBuildChunk struct {
	lo, hi int
	// dict and names map chunk-local term IDs, in order of first appearance
	// within the chunk; remap translates them to the index's global IDs.
	dict  map[string]int32
	names []string
	remap []int32
	// tokens holds every row's token IDs back to back: chunk-local IDs after
	// tokenize, then global IDs sorted within each row after index.
	// rowTokens/rowTerms are each row's token and distinct-term counts.
	tokens    []int32
	rowTokens []int32
	rowTerms  []int32
	// tokenStart/termStart are the chunk's offsets in the index arenas.
	tokenStart, termStart int
	numDocs, totalLen     int
	panicked              any
	didPanic              bool
}

func (c *ftsBuildChunk) tokenize(rows [][]any, cols []int) {
	c.dict = make(map[string]int32)
	c.rowTokens = make([]int32, c.hi-c.lo)
	for ri := c.lo; ri < c.hi; ri++ {
		before := len(c.tokens)
		ftsVisitRowTokens(rows[ri], cols, func(term string) {
			id, ok := c.dict[term]
			if !ok {
				id = int32(len(c.names))
				c.dict[term] = id
				c.names = append(c.names, term)
			}
			c.tokens = append(c.tokens, id)
		})
		c.rowTokens[ri-c.lo] = int32(len(c.tokens) - before)
		// Extrapolate the chunk's arena from its first rows, as the
		// sequential build does, instead of growing it by repeated doubling.
		if ri-c.lo+1 == ftsArenaEstimateAfter {
			perRow := len(c.tokens)/ftsArenaEstimateAfter + 1
			c.tokens = ftsReserve(c.tokens, perRow*(c.hi-c.lo)*5/4)
		}
	}
}

// index writes the chunk's tokens, as global IDs, to its slice of the
// index's token arena, then sorts each row in the chunk's own buffer and
// counts its distinct terms.
func (c *ftsBuildChunk) index(docTokens []int32) {
	c.rowTerms = make([]int32, len(c.rowTokens))
	out := docTokens[c.tokenStart : c.tokenStart+len(c.tokens)]
	pos := 0
	for r, n := range c.rowTokens {
		row := c.tokens[pos : pos+int(n)]
		for i, local := range row {
			id := c.remap[local]
			out[pos+i] = id
			row[i] = id
		}
		pos += int(n)
		slices.Sort(row)
		distinct := int32(0)
		for i := range row {
			if i == 0 || row[i] != row[i-1] {
				distinct++
			}
		}
		c.rowTerms[r] = distinct
	}
}

// terms run-length encodes each row's sorted IDs into the chunk's slice of
// the term arenas and fills its rows' document entries.
func (c *ftsBuildChunk) terms(index *storage.FTSIndex) {
	tokenPos, termPos := c.tokenStart, c.termStart
	pos := 0
	for r, n := range c.rowTokens {
		row := c.tokens[pos : pos+int(n)]
		pos += int(n)
		if n > 0 {
			index.Docs[c.lo+r] = storage.FTSDocument{
				TermStart: int32(termPos), TermCount: c.rowTerms[r],
				TokenStart: int32(tokenPos), TokenCount: n,
				DocLen: float64(n), Valid: true,
			}
			c.numDocs++
			c.totalLen += int(n)
		}
		for i := 0; i < len(row); {
			j := i + 1
			for j < len(row) && row[j] == row[i] {
				j++
			}
			index.DocTermIDs[termPos] = row[i]
			index.DocTermCounts[termPos] = int32(j - i)
			termPos++
			i = j
		}
		tokenPos += int(n)
	}
	c.tokens = nil
}

// ftsRunChunks runs fn for every chunk on its own goroutine and re-raises a
// worker panic on the calling goroutine, where the executor's own recovery
// can see it.
func ftsRunChunks(chunks []ftsBuildChunk, fn func(*ftsBuildChunk)) {
	var wg sync.WaitGroup
	for i := range chunks {
		wg.Add(1)
		go func(c *ftsBuildChunk) {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					c.panicked, c.didPanic = r, true
				}
			}()
			fn(c)
		}(&chunks[i])
	}
	wg.Wait()
	for i := range chunks {
		if chunks[i].didPanic {
			panic(chunks[i].panicked)
		}
	}
}

// ftsBuildFreshParallel is the packed (fresh, bulk) branch of
// ftsExtendPersistent run across workers. It produces exactly the index the
// sequential loop does: a term's global ID is its order of first appearance
// in row order, and assigning IDs chunk by chunk, each chunk in its own
// first-appearance order, yields that same order, because every term first
// appears in the earliest chunk containing it. Everything after the ID
// assignment is per row and therefore independent of the partitioning, and
// each chunk writes straight into its own region of the index arenas.
//
// index must be empty, with its maps allocated and Docs sized to the table,
// as ftsExtendPersistent arranges before choosing the packed path.
func ftsBuildFreshParallel(table *storage.Table, cols []int, index *storage.FTSIndex, workers int) {
	rows := table.Rows
	chunks := make([]ftsBuildChunk, workers)
	per := (len(rows) + workers - 1) / workers
	for w := range chunks {
		lo := min(w*per, len(rows))
		chunks[w].lo, chunks[w].hi = lo, min(lo+per, len(rows))
	}
	ftsRunChunks(chunks, func(c *ftsBuildChunk) { c.tokenize(rows, cols) })

	var termNames []string
	totalTokens := 0
	for w := range chunks {
		c := &chunks[w]
		c.remap = make([]int32, len(c.names))
		for local, term := range c.names {
			id, ok := index.TermIDs[term]
			if !ok {
				id = int32(len(termNames))
				index.TermIDs[term] = id
				termNames = append(termNames, term)
			}
			c.remap[local] = id
		}
		c.dict, c.names = nil, nil
		c.tokenStart = totalTokens
		totalTokens += len(c.tokens)
	}
	// Same headroom the sequential build reserves, so the first append-only
	// extension does not immediately copy the arenas. Empty arenas stay nil,
	// as they do there.
	if totalTokens > 0 {
		index.DocTokenIDs = make([]int32, totalTokens, totalTokens+totalTokens/4)
	}
	ftsRunChunks(chunks, func(c *ftsBuildChunk) { c.index(index.DocTokenIDs) })

	totalTerms := 0
	for w := range chunks {
		chunks[w].termStart = totalTerms
		for _, n := range chunks[w].rowTerms {
			totalTerms += int(n)
		}
	}
	if totalTerms > 0 {
		index.DocTermIDs = make([]int32, totalTerms, totalTerms+totalTerms/4)
		index.DocTermCounts = make([]int32, totalTerms, totalTerms+totalTerms/4)
	}
	ftsRunChunks(chunks, func(c *ftsBuildChunk) { c.terms(index) })

	// Token counts are integers, so summing per chunk is exact and matches
	// the sequential per-row accumulation.
	totalLen := 0
	for w := range chunks {
		index.NumDocs += chunks[w].numDocs
		totalLen += chunks[w].totalLen
	}
	index.TotalDocLen = float64(totalLen)
	if index.NumDocs > 0 {
		index.AvgDocLen = index.TotalDocLen / float64(index.NumDocs)
	}
	postingSizes := make([]int, len(termNames))
	for _, id := range index.DocTermIDs {
		postingSizes[id]++
	}
	ftsBuildPackedPostings(index, termNames, postingSizes)
}
