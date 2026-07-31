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
	"sort"
	"strings"
)

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
