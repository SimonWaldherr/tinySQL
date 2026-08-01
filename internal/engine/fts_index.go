package engine

// Inverted-index support for FTS_SEARCH.
//
// FTS_SEARCH used to score every document in the corpus on every query: a term
// present in 3 of 20000 chunks cost exactly as much as a term present in all of
// them. For a RAG corpus that is the dominant cost of the whole retrieval path,
// because HYBRID_SEARCH runs a BM25 pass on every question (measured: the
// lexical branch was ~98% of hybrid retrieval time).
//
// Two structures fix that, both built inside the existing tokenized-document
// cache and invalidated by table.Version exactly like it:
//
//   - postings: term -> ascending row indices containing that term. A query's
//     candidate set is derived from these, so documents that cannot match are
//     never scored.
//   - the postings map's key set doubles as the term dictionary. A wildcard is
//     resolved against it ONCE per query (thousands of unique terms) instead of
//     against every token of every document (millions of token instances).
//
// Why restricting candidates cannot change results: ftsScoredLess defines a
// total order on (score desc, rowIdx asc) and ftsPushTopK admits candidates
// only by comparing against the heap root under that same order. The resulting
// top-k is therefore a function of the candidate *set*, not of the order
// candidates are offered in. So long as the candidate set is a superset of the
// true match set and every candidate is still verified with ftsMatchNode and
// scored with ftsScoreNode, the output is identical to a full scan.

import (
	"context"
	"fmt"
	"runtime"
	"sort"
	"strings"
	"sync"
)

const (
	// Thresholds for parallelizing the BM25 scan, mirroring
	// vecSearchParallelMinRows/vecSearchParallelChunkRows in vector_search.go:
	// below these sizes the goroutine and merge overhead outweighs the scan.
	ftsScanParallelMinDocs   = 4096
	ftsScanParallelChunkDocs = 2048
)

// ftsScanWorkerCount picks a worker count for scanning docs documents.
func ftsScanWorkerCount(docs int) int {
	if docs < ftsScanParallelMinDocs {
		return 1
	}
	workers := runtime.GOMAXPROCS(0)
	if workers < 2 {
		return 1
	}
	if maxByDocs := (docs + ftsScanParallelChunkDocs - 1) / ftsScanParallelChunkDocs; workers > maxByDocs {
		workers = maxByDocs
	}
	if workers < 2 {
		return 1
	}
	return workers
}

// ftsScanTopK matches and scores the documents a query could hit and returns the
// k best, scanning in parallel across workers when there are enough documents to
// justify it. The vector branch has scanned in parallel for some time
// (vecSearchTopK); the BM25 branch was single-threaded, which made it the
// long pole of hybrid retrieval on a many-core machine whenever the query's
// terms were too common for the postings index to narrow.
//
// Partitioning cannot affect the result. ftsScoredLess is a total order on
// (score desc, rowIdx asc) and every heap operation here — per-worker pushes and
// the final merge — uses it, so the k best documents and their order depend only
// on which documents were scanned, never on how they were divided up or in which
// order workers finished.
func ftsScanTopK(ctx context.Context, cache ftsDocCacheEntry, node *ftsQueryNode, idf ftsIDFFunc, rows []int32, restricted bool, k int) ([]ftsScored, error) {
	total := len(cache.docs)
	if restricted {
		total = len(rows)
	}

	workers := ftsScanWorkerCount(total)
	if workers == 1 {
		h, err := ftsScanRange(ctx, cache, node, idf, rows, restricted, 0, total, k)
		if err != nil {
			return nil, err
		}
		return ftsTopKFromHeap(&h, k), nil
	}

	type workerResult struct {
		heapRows ftsScoredHeap
		err      error
	}
	results := make([]workerResult, workers)
	var wg sync.WaitGroup
	chunk := (total + workers - 1) / workers

	for worker := 0; worker < workers; worker++ {
		start := worker * chunk
		end := start + chunk
		if end > total {
			end = total
		}
		if start >= end {
			continue
		}
		wg.Add(1)
		go func(worker, start, end int) {
			defer wg.Done()
			// Contain a panic as an ordinary worker error rather than letting it
			// crash the process, matching vecSearchTopK's behavior.
			defer func() {
				if r := recover(); r != nil {
					results[worker].err = fmt.Errorf("FTS_SEARCH: worker panic: %v", r)
				}
			}()
			h, err := ftsScanRange(ctx, cache, node, idf, rows, restricted, start, end, k)
			results[worker] = workerResult{heapRows: h, err: err}
		}(worker, start, end)
	}
	wg.Wait()

	merged := &ftsScoredHeap{}
	for i := range results {
		if results[i].err != nil {
			return nil, results[i].err
		}
		for _, sr := range ftsTopKFromHeap(&results[i].heapRows, k) {
			ftsPushTopK(merged, sr.rowIdx, sr.score, k)
		}
	}
	return ftsTopKFromHeap(merged, k), nil
}

// ftsEvalNode decides whether a document matches and what it scores in a single
// walk of the query tree.
//
// ftsMatchNode and ftsScoreNode walk the same tree and perform the same
// freq[term] lookups, so every matching document used to hash and compare each
// query term twice. Those lookups were 68% of a RAG SELECT's runtime, and half
// of them were redundant.
//
// The results are identical to calling the two functions in sequence, which
// TestFTSCandidateRestrictionMatchesFullScan verifies: its oracle still uses the
// separate functions, so any divergence here fails that test. The two remain the
// implementation for the single-text scalar functions (FTS_MATCH/FTS_RANK),
// which have no corpus and cannot use the bound-IDF form.
//
// Short-circuiting is preserved where it is observable. AND returns as soon as
// its left side fails to match, because a non-matching document's score is
// discarded by the caller. OR must still evaluate both sides even once the left
// matches, since its score is the maximum of the two — exactly what
// ftsScoreNode already did.
func ftsEvalNode(cache ftsDocCacheEntry, doc ftsCachedDoc, node *ftsQueryNode, normDocLen float64) (bool, float64) {
	if node == nil {
		return false, 0
	}
	// Mirrors ftsScoreNode's arithmetic exactly, including the order of
	// operations: float multiplication is not associative, so scaling by IDF
	// after the division is load-bearing for bit-identical scores.
	lengthNorm := bm25K1 * (1 - bm25B + bm25B*normDocLen)
	score := func(weight float64, f int) float64 {
		tf := float64(f)
		if tf == 0 {
			return 0
		}
		s := (tf * (bm25K1 + 1)) / (tf + lengthNorm)
		return s * weight
	}

	switch node.op {
	case "TERM":
		f := cache.termFrequency(doc, node.termID)
		return f > 0, score(node.termIDF, f)

	case ftsExpandedOp:
		matched := false
		var sum float64
		for i, id := range node.termIDNs {
			f := cache.termFrequency(doc, id)
			if f == 0 {
				continue
			}
			matched = true
			sum += score(node.termIDFs[i], f)
		}
		return matched, sum

	case "PHRASE":
		if len(node.phrase) == 0 {
			// ftsMatchNode treats an empty phrase as matching; ftsScoreNode
			// scores it 0. Both are preserved.
			return true, 0
		}
		if !ftsPhraseMatchIDs(node.termIDNs, cache.docTokens(doc)) {
			return false, 0
		}
		var sum float64
		for i, id := range node.termIDNs {
			sum += score(node.termIDFs[i], cache.termFrequency(doc, id))
		}
		return true, sum * phraseMatchBonus

	case "AND":
		leftMatched, leftScore := ftsEvalNode(cache, doc, node.left, normDocLen)
		if !leftMatched {
			return false, 0
		}
		rightMatched, rightScore := ftsEvalNode(cache, doc, node.right, normDocLen)
		if !rightMatched {
			return false, 0
		}
		return true, leftScore + rightScore

	case "OR":
		leftMatched, leftScore := ftsEvalNode(cache, doc, node.left, normDocLen)
		rightMatched, rightScore := ftsEvalNode(cache, doc, node.right, normDocLen)
		if leftScore > rightScore {
			return leftMatched || rightMatched, leftScore
		}
		return leftMatched || rightMatched, rightScore

	case "NOT":
		matched, _ := ftsEvalNode(cache, doc, node.operand, normDocLen)
		return !matched, 0

	default:
		// PREFIX/WILDCARD reach here only if the tree was never expanded, which
		// cannot happen on the corpus path (ftsExpandQuery runs first and
		// ftsBindIDF resolves the result). Treat as no match rather than
		// silently scoring, so a future caller that skips expansion fails
		// visibly in tests instead of returning wrong scores.
		return false, 0
	}
}

// ftsPhraseMatchIDs reports whether tokens contains phrase as a consecutive
// subsequence, comparing term ids. Same algorithm as ftsPhraseMatch, on int32s
// instead of strings.
func ftsPhraseMatchIDs(phrase []int32, tokens []int32) bool {
	if len(phrase) == 0 || len(phrase) > len(tokens) {
		return len(phrase) == 0
	}
	for _, id := range phrase {
		if id < 0 {
			return false // a term absent from the corpus cannot appear
		}
	}
	for i := 0; i <= len(tokens)-len(phrase); i++ {
		match := true
		for j, p := range phrase {
			if tokens[i+j] != p {
				match = false
				break
			}
		}
		if match {
			return true
		}
	}
	return false
}

// ftsScanRange scores the half-open slice [start, end) of either the candidate
// row list (restricted) or the whole corpus, returning a size-k heap.
func ftsScanRange(ctx context.Context, cache ftsDocCacheEntry, node *ftsQueryNode, idf ftsIDFFunc, rows []int32, restricted bool, start, end, k int) (ftsScoredHeap, error) {
	heapRows := make(ftsScoredHeap, 0, k)
	for i := start; i < end; i++ {
		if i&1023 == 0 {
			if err := checkCtx(ctx); err != nil {
				return nil, err
			}
		}
		ri := i
		if restricted {
			ri = int(rows[i])
			if ri < 0 || ri >= len(cache.docs) {
				continue
			}
		}
		doc := cache.docs[ri]
		if !doc.valid {
			continue
		}
		normDocLen := doc.docLen
		if cache.avgDocLen > 0 {
			normDocLen = doc.docLen / cache.avgDocLen
		}
		if node == nil {
			// A nil query tree previously bypassed the match check and scored
			// every valid document 0. FTS_SEARCH rejects an empty query before
			// reaching here, but preserve the behavior for direct callers.
			ftsPushTopK(&heapRows, ri, 0, k)
			continue
		}
		matched, score := ftsEvalNode(cache, doc, node, normDocLen)
		if !matched {
			continue
		}
		ftsPushTopK(&heapRows, ri, score, k)
	}
	return heapRows, nil
}

// ftsCandidates is the set of rows a query could possibly match.
//
// unrestricted means "no sound restriction was derivable, scan everything" —
// the safe fallback, and the only correct answer for a query whose match set is
// defined by negation (see ftsQueryCandidates).
type ftsCandidates struct {
	rows         []int32
	unrestricted bool
}

func ftsUnrestricted() ftsCandidates { return ftsCandidates{unrestricted: true} }

// ftsExpandedOp is a query node produced only for the corpus-backed search
// path: a PREFIX or WILDCARD atom with its matching dictionary terms already
// resolved and sorted.
//
// It exists for two reasons. Speed: the original nodes test their pattern
// against every token of every candidate document, whereas the dictionary is
// swept once per query. Determinism: PREFIX/WILDCARD scoring sums a float64 per
// matching token while iterating a Go map, and map iteration order is
// randomized, so identical queries against an unchanged corpus returned
// slightly different scores from run to run. Summing over a sorted term list
// makes the score reproducible.
const ftsExpandedOp = "EXPANDED"

// ftsExpandQuery rewrites PREFIX and WILDCARD nodes into ftsExpandedOp nodes
// whose terms are the dictionary terms they match, sorted.
//
// The rewrite is score-preserving by construction. Every token of every
// document is a dictionary key, so {tokens of doc matching pattern} equals
// {dictionary terms matching pattern} ∩ {tokens of doc}; scoring the latter
// sums exactly the same (term, frequency) pairs. Terms absent from a document
// contribute termScore(t, 0) == 0 and are skipped.
//
// The returned tree shares unmodified subtrees with the input. Nodes are only
// ever replaced, never mutated in place, because a parsed query tree may be
// reused (nothing here may disturb the caller's node).
func ftsExpandQuery(node *ftsQueryNode, postings map[string][]int32) *ftsQueryNode {
	if node == nil {
		return nil
	}
	switch node.op {
	case "PREFIX":
		return &ftsQueryNode{op: ftsExpandedOp, phrase: ftsTermsWithPrefix(postings, node.prefix)}
	case "WILDCARD":
		return &ftsQueryNode{op: ftsExpandedOp, phrase: ftsTermsMatchingPattern(postings, node.pattern)}
	case "AND", "OR":
		left := ftsExpandQuery(node.left, postings)
		right := ftsExpandQuery(node.right, postings)
		if left == node.left && right == node.right {
			return node
		}
		return &ftsQueryNode{op: node.op, left: left, right: right}
	case "NOT":
		operand := ftsExpandQuery(node.operand, postings)
		if operand == node.operand {
			return node
		}
		return &ftsQueryNode{op: "NOT", operand: operand}
	default:
		// TERM and PHRASE need no expansion.
		return node
	}
}

// ftsBindIDF returns a copy of node with every term's inverse document
// frequency precomputed, so scoring a document costs a multiply instead of a
// closure call, a postings lookup and a math.Log per term.
//
// IDF is a function of the term and the corpus alone, so it is invariant across
// the documents a single query scores — yet ftsScoreNode used to evaluate it
// once per (term, document) pair. On a 20,000-document corpus a four-term query
// paid 80,000 logarithms to compute four distinct values.
//
// Nodes are copied rather than annotated in place: a caller's parsed tree may be
// reused across queries and corpora, and IDF is only valid for the corpus it was
// derived from. PREFIX and WILDCARD nodes are left unbound — on this path
// ftsExpandQuery has already replaced them, and where it has not (the
// single-text scalar functions) there is no corpus to bind against.
func ftsBindIDF(node *ftsQueryNode, idf ftsIDFFunc, termIDs map[string]int32) *ftsQueryNode {
	if node == nil || idf == nil {
		return node
	}
	// A term the corpus has never seen gets id -1, which no document run
	// contains, so it simply never matches — the same outcome as a zero
	// frequency from the string map.
	lookup := func(term string) int32 {
		if id, ok := termIDs[term]; ok {
			return id
		}
		return -1
	}
	switch node.op {
	case "TERM":
		return &ftsQueryNode{op: "TERM", term: node.term,
			idfBound: true, termIDF: idf(node.term), termID: lookup(node.term)}
	case "PHRASE", ftsExpandedOp:
		weights := make([]float64, len(node.phrase))
		ids := make([]int32, len(node.phrase))
		for i, term := range node.phrase {
			weights[i] = idf(term)
			ids[i] = lookup(term)
		}
		return &ftsQueryNode{op: node.op, phrase: node.phrase,
			idfBound: true, termIDFs: weights, termIDNs: ids}
	case "AND", "OR":
		return &ftsQueryNode{op: node.op,
			left:  ftsBindIDF(node.left, idf, termIDs),
			right: ftsBindIDF(node.right, idf, termIDs)}
	case "NOT":
		return &ftsQueryNode{op: "NOT", operand: ftsBindIDF(node.operand, idf, termIDs)}
	default:
		return node
	}
}

// ftsTermsWithPrefix returns the sorted dictionary terms starting with prefix.
func ftsTermsWithPrefix(postings map[string][]int32, prefix string) []string {
	var out []string
	for term := range postings {
		if strings.HasPrefix(term, prefix) {
			out = append(out, term)
		}
	}
	sort.Strings(out)
	return out
}

// ftsTermsMatchingPattern returns the sorted dictionary terms matching pattern.
func ftsTermsMatchingPattern(postings map[string][]int32, pattern []ftsWildcardAtom) []string {
	var out []string
	for term := range postings {
		if ftsWildcardMatch(pattern, term) {
			out = append(out, term)
		}
	}
	sort.Strings(out)
	return out
}

// ftsQueryCandidates derives a superset of the rows node can match.
//
// The rule per node type:
//
//	TERM      postings for the term
//	EXPANDED  union of postings over the resolved terms
//	PHRASE    intersection over the phrase's terms — a phrase requires all of
//	          them to be present (an empty phrase matches everything)
//	AND       intersection of both sides, treating unrestricted as identity
//	OR        union of both sides; unrestricted if either side is
//	NOT        *unrestricted* — a document containing none of the query's terms
//	          can satisfy a negation, so no postings list can bound it
//	PREFIX/WILDCARD
//	          unrestricted; these only survive un-expanded on paths with no
//	          dictionary, and guessing would be unsound
//
// NOT propagating unrestricted through OR is what keeps `a OR NOT b` correct
// (full scan), while AND's identity treatment still restricts the common and
// useful `a AND NOT b` to a's postings.
//
// maxUseful is the size past which a candidate list stops being worth building:
// a set covering most of the corpus saves no scoring work, and materializing it
// costs allocation and copying that a plain scan does not. Exceeding it yields
// unrestricted, which is always a sound answer. Bailing out early matters — a
// query of several common terms would otherwise union its way to a
// corpus-sized list only for the caller to discard it and scan anyway.
func ftsQueryCandidates(node *ftsQueryNode, postings map[string][]int32, numRows int) ftsCandidates {
	return ftsQueryCandidatesLimit(node, postings, numRows, numRows/2)
}

func ftsQueryCandidatesLimit(node *ftsQueryNode, postings map[string][]int32, numRows, maxUseful int) ftsCandidates {
	if node == nil {
		return ftsCandidates{}
	}
	switch node.op {
	case "TERM":
		p := postings[node.term]
		if len(p) > maxUseful {
			return ftsUnrestricted()
		}
		return ftsCandidates{rows: p}

	case ftsExpandedOp:
		if len(node.phrase) == 0 {
			// No dictionary term matches the pattern, so nothing can match.
			return ftsCandidates{}
		}
		lists := make([][]int32, 0, len(node.phrase))
		total := 0
		for _, term := range node.phrase {
			if p := postings[term]; len(p) > 0 {
				lists = append(lists, p)
				total += len(p)
				// Overlap could still leave the union small, so this is
				// pessimistic; it trades a rare missed restriction for never
				// building a union larger than a scan.
				if total > maxUseful {
					return ftsUnrestricted()
				}
			}
		}
		return ftsCandidates{rows: ftsUnionAll(lists, numRows)}

	case "PHRASE":
		if len(node.phrase) == 0 {
			return ftsUnrestricted()
		}
		// The intersection cannot exceed the shortest postings list, so that
		// length decides up front whether a restriction is worth materializing.
		// Checking first avoids allocating a corpus-sized intermediate for a
		// phrase made of common terms.
		shortest := -1
		for _, term := range node.phrase {
			n := len(postings[term])
			if n == 0 {
				return ftsCandidates{} // a missing phrase term means no match
			}
			if shortest < 0 || n < shortest {
				shortest = n
			}
		}
		if shortest > maxUseful {
			return ftsUnrestricted()
		}
		var acc []int32
		for i, term := range node.phrase {
			p := postings[term]
			if i == 0 {
				acc = p
				continue
			}
			acc = ftsIntersect(acc, p)
			if len(acc) == 0 {
				return ftsCandidates{}
			}
		}
		return ftsCandidates{rows: acc}

	case "AND":
		// An intersection only shrinks, so each side is resolved against the
		// full corpus bound rather than maxUseful: a side that is individually
		// too broad to be worth materializing can still be intersected away by
		// a selective sibling.
		left := ftsQueryCandidatesLimit(node.left, postings, numRows, numRows)
		right := ftsQueryCandidatesLimit(node.right, postings, numRows, numRows)
		switch {
		case left.unrestricted && right.unrestricted:
			return ftsUnrestricted()
		case left.unrestricted:
			return right
		case right.unrestricted:
			return left
		}
		// The intersection cannot exceed the shorter side. If even that is too
		// broad to be worth materializing, skip it: the result would very likely
		// be discarded for a full scan anyway, and building it costs an
		// allocation proportional to the corpus. When one side *is* selective,
		// the allocation is bounded by it, which is the case worth serving.
		if len(left.rows) > maxUseful && len(right.rows) > maxUseful {
			return ftsUnrestricted()
		}
		return ftsCandidates{rows: ftsIntersect(left.rows, right.rows)}

	case "OR":
		left := ftsQueryCandidatesLimit(node.left, postings, numRows, maxUseful)
		if left.unrestricted {
			// A union only grows, so one unrestricted side settles it without
			// resolving the other.
			return ftsUnrestricted()
		}
		right := ftsQueryCandidatesLimit(node.right, postings, numRows, maxUseful)
		if right.unrestricted {
			return ftsUnrestricted()
		}
		if len(left.rows)+len(right.rows) > maxUseful {
			return ftsUnrestricted()
		}
		return ftsCandidates{rows: ftsUnion(left.rows, right.rows)}

	case "NOT":
		return ftsUnrestricted()

	default:
		return ftsUnrestricted()
	}
}

// ftsUnion merges two ascending, duplicate-free row lists.
func ftsUnion(a, b []int32) []int32 {
	if len(a) == 0 {
		return b
	}
	if len(b) == 0 {
		return a
	}
	out := make([]int32, 0, len(a)+len(b))
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		switch {
		case a[i] < b[j]:
			out = append(out, a[i])
			i++
		case a[i] > b[j]:
			out = append(out, b[j])
			j++
		default:
			out = append(out, a[i])
			i++
			j++
		}
	}
	out = append(out, a[i:]...)
	return append(out, b[j:]...)
}

// ftsIntersect intersects two ascending, duplicate-free row lists.
func ftsIntersect(a, b []int32) []int32 {
	if len(a) == 0 || len(b) == 0 {
		return nil
	}
	// Iterate the shorter list's span; the two-pointer walk is O(len(a)+len(b))
	// either way, but the output cannot exceed the shorter input.
	shorter := len(a)
	if len(b) < shorter {
		shorter = len(b)
	}
	out := make([]int32, 0, shorter)
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		switch {
		case a[i] < b[j]:
			i++
		case a[i] > b[j]:
			j++
		default:
			out = append(out, a[i])
			i++
			j++
		}
	}
	return out
}

// ftsUnionAll unions many ascending row lists.
//
// Pairwise merging would be O(total × lists) in the worst case; a broad
// wildcard can resolve to a thousand terms. Marking a presence bitmap and then
// scanning it in row order is O(total + numRows) and yields ascending output
// with no sort. For a handful of short lists the bitmap allocation is not worth
// it, so those merge pairwise.
func ftsUnionAll(lists [][]int32, numRows int) []int32 {
	switch len(lists) {
	case 0:
		return nil
	case 1:
		return lists[0]
	}

	total := 0
	for _, l := range lists {
		total += len(l)
	}
	if len(lists) <= 4 && total < numRows/8 {
		out := lists[0]
		for _, l := range lists[1:] {
			out = ftsUnion(out, l)
		}
		return out
	}

	present := make([]bool, numRows)
	distinct := 0
	for _, l := range lists {
		for _, row := range l {
			if row >= 0 && int(row) < numRows && !present[row] {
				present[row] = true
				distinct++
			}
		}
	}
	out := make([]int32, 0, distinct)
	for row := 0; row < numRows; row++ {
		if present[row] {
			out = append(out, int32(row))
		}
	}
	return out
}

// ftsCandidateScanIsCheaper reports whether iterating an explicit candidate
// list beats scanning the corpus.
//
// Candidate iteration touches len(rows) documents; a full scan touches numRows.
// Once a candidate set covers a large share of the corpus, the indirection buys
// nothing and the full scan has better locality, so the caller falls back to it.
// Either path returns identical results, so this is purely a cost choice.
func ftsCandidateScanIsCheaper(rows []int32, numRows int) bool {
	if numRows <= 0 {
		return false
	}
	return len(rows)*2 < numRows
}
