package pager

import (
	"bytes"
	"fmt"
	"math/rand"
	"path/filepath"
	"testing"
)

// ───────────────────────────────────────────────────────────────────────────
// Regression suite for the count-balanced leaf/internal split bug.
// ───────────────────────────────────────────────────────────────────────────
//
// The reported failure was a real regional MBTiles tileset (158 MiB, 6,646
// map rows, 11,465 images rows, 32-char tile_id, 409 BLOBs in the 1,400-2,500
// byte band) failing to import into ModePagedIndex with:
//
//	insert row 10784: split right insert: btree page full: need 1569, have 1536 free
//
// The root cause was a count-balanced leaf split (`mid := len(merged) / 2`):
// with variable-size records, a count midpoint can leave too many large
// records on one side even though both sides together fit in two pages. The
// fix (leafSplitIndex/internalSplitIndex, and the leafEntryNeedsOverflow
// decision they depend on) balances by encoded bytes instead. These tests
// exercise the exact boundary sizes named in the report, every key insertion
// order, replace/delete cycles that must not leak overflow pages, and
// multi-level internal-split invariants -- on top of the coverage already in
// btree_large_records_test.go, which covers the same class of bug with a
// different size distribution.

// exactBoundaryPayloadSizes are the sizes named in the bug report itself.
// 1,536/1,537 straddle 1,536 = 1,024+512 (a plausible slot/alignment
// boundary), 1,569 is the literal "need 1569" from the failure, and 1,400 /
// 1,800 / 2,500 bracket the reported "409 BLOBs in the 1,400-2,500 byte
// range". 256 and 4,096 cover a small inline record and an exact page-size
// value; 50,000 always overflows.
var exactBoundaryPayloadSizes = []int{256, 1400, 1536, 1537, 1569, 1800, 2500, 4096, 50_000}

func exactBoundaryPayload(i, size int) []byte {
	payload := make([]byte, size)
	for j := range payload {
		payload[j] = byte(i*31 + j*17 + size)
	}
	return payload
}

// TestLeafEntryNeedsOverflowBoundary pins the overflow decision at the exact
// sizes from the bug report, for the default page size and a short key. A
// wrong decision here (unnecessarily overflowing a record that fits, or
// inlining one that does not) is the seed of every downstream split failure.
func TestLeafEntryNeedsOverflowBoundary(t *testing.T) {
	key := []byte("tile/00000000")
	capacity := btreeRecordCapacity(DefaultPageSize)
	threshold := overflowThresholdFor(DefaultPageSize)

	cases := []struct {
		size         int
		wantOverflow bool
	}{
		{256, false},
		{1400, false},
		{1536, false},
		{1537, false},
		{1569, false},
		{1800, false},
		{2500, true},
		{4096, true},
		{50_000, true},
	}
	for _, c := range cases {
		value := make([]byte, c.size)
		got := leafEntryNeedsOverflow(DefaultPageSize, key, value)
		if got != c.wantOverflow {
			t.Errorf("size %d: leafEntryNeedsOverflow = %v, want %v", c.size, got, c.wantOverflow)
		}
		// Whichever way it decided, the resulting record must actually fit an
		// empty page -- that is the entire point of the decision.
		var footprint int
		if got {
			footprint = leafRecordFootprint(LeafEntry{Key: key, Overflow: true, TotalSize: uint32(len(value))})
		} else {
			footprint = leafRecordFootprint(LeafEntry{Key: key, Value: value})
		}
		if footprint > capacity {
			t.Errorf("size %d: chosen representation still does not fit an empty page (%d > %d)", c.size, footprint, capacity)
		}
	}

	// A tiny value must never be pushed to overflow, no matter how the
	// threshold is tuned.
	if leafEntryNeedsOverflow(DefaultPageSize, key, []byte("x")) {
		t.Error("a 1-byte value must stay inline")
	}

	// A long key can force overflow even for a value well under the
	// threshold: this is the exact shape TestBTreeOverflowDecisionUsesEncodedLeafRecordSize
	// checks end-to-end; here it is pinned at the decision-function level.
	longKey := bytes.Repeat([]byte("k"), 6_700)
	modestValue := bytes.Repeat([]byte("v"), 1_500)
	if !leafEntryNeedsOverflow(DefaultPageSize, longKey, modestValue) {
		t.Error("long key + modest value should overflow: the combined inline footprint cannot fit an empty page")
	}
	if threshold <= 0 {
		t.Fatalf("sanity: overflowThresholdFor returned non-positive %d", threshold)
	}
}

// TestBTreeExactBoundarySizesAllKeyOrders is the direct regression for the
// reported failure: every payload size named in the bug report, inserted in
// sorted, reverse and random key order, at a scale that forces many
// recursive leaf and internal splits. Before the byte-balanced split fix,
// this reproduced "btree page full" partway through a split's right-page
// rebuild; it must now succeed and round-trip exactly through a durable
// close and read-only reopen.
func TestBTreeExactBoundarySizesAllKeyOrders(t *testing.T) {
	const rows = 10_240
	for _, orderName := range []string{"sorted", "reverse", "random"} {
		t.Run(orderName, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "tiles.pages")
			p, err := OpenPager(PagerConfig{DBPath: path, PageSize: DefaultPageSize, MaxCachePages: 64})
			if err != nil {
				t.Fatal(err)
			}

			txID, err := p.BeginTx()
			if err != nil {
				t.Fatal(err)
			}
			bt, err := CreateBTree(p, txID)
			if err != nil {
				t.Fatal(err)
			}

			order := make([]int, rows)
			for i := range order {
				order[i] = i
			}
			switch orderName {
			case "reverse":
				for i, j := 0, len(order)-1; i < j; i, j = i+1, j-1 {
					order[i], order[j] = order[j], order[i]
				}
			case "random":
				rand.New(rand.NewSource(7)).Shuffle(rows, func(i, j int) { order[i], order[j] = order[j], order[i] })
			}

			want := make(map[string][]byte, rows)
			for _, i := range order {
				key := []byte(fmt.Sprintf("k/%08d", i))
				size := exactBoundaryPayloadSizes[i%len(exactBoundaryPayloadSizes)]
				value := exactBoundaryPayload(i, size)
				want[string(key)] = value
				if err := bt.Insert(txID, key, value); err != nil {
					t.Fatalf("%s order: insert row %d (size %d): %v", orderName, i, size, err)
				}
			}
			root := bt.Root()
			if err := p.CommitTx(txID); err != nil {
				t.Fatal(err)
			}

			leaves, internals := countBTreePages(t, p, root)
			if leaves < 50 {
				t.Fatalf("%s order: expected many leaf splits, got %d leaves", orderName, leaves)
			}
			if internals < 2 {
				t.Fatalf("%s order: expected at least one internal split, got %d internal pages", orderName, internals)
			}
			if err := p.Close(); err != nil {
				t.Fatal(err)
			}

			reader, err := OpenPager(PagerConfig{DBPath: path, PageSize: DefaultPageSize, MaxCachePages: 32, ReadOnly: true})
			if err != nil {
				t.Fatalf("%s order: read-only reopen: %v", orderName, err)
			}
			defer reader.Close()
			readTree := NewBTree(reader, root)

			seen := 0
			var prevKey []byte
			if err := readTree.ScanRange(nil, nil, func(key, value []byte) bool {
				if prevKey != nil && bytes.Compare(prevKey, key) >= 0 {
					t.Errorf("%s order: scan order violated at %q after %q", orderName, key, prevKey)
				}
				prevKey = append(prevKey[:0], key...)
				expected, ok := want[string(key)]
				if !ok {
					t.Errorf("%s order: unexpected key %q", orderName, key)
					return true
				}
				if !bytes.Equal(value, expected) {
					t.Errorf("%s order: value mismatch for %q: got %d bytes, want %d", orderName, key, len(value), len(expected))
				}
				seen++
				return true
			}); err != nil {
				t.Fatal(err)
			}
			if seen != rows {
				t.Fatalf("%s order: scan returned %d rows, want %d", orderName, seen, rows)
			}

			for i := 0; i < rows; i++ {
				key := []byte(fmt.Sprintf("k/%08d", i))
				got, found, err := readTree.Get(key)
				if err != nil || !found {
					t.Fatalf("%s order: get %q: found=%v err=%v", orderName, key, found, err)
				}
				if !bytes.Equal(got, want[string(key)]) {
					t.Fatalf("%s order: get %q returned different bytes", orderName, key)
				}
			}
			if _, found, err := readTree.Get([]byte("k/absent!!")); err != nil || found {
				t.Fatalf("%s order: negative lookup found=%v err=%v, want not found", orderName, found, err)
			}
			if err := verifyBTreePages(reader, root); err != nil {
				t.Fatalf("%s order: page integrity: %v", orderName, err)
			}
		})
	}
}

// TestBTreeReplaceDeleteInsertOverflowSequenceNoLeak replaces the same key
// many times with a mix of inline and overflow-sized values, interleaved with
// a delete and reinsert, and checks the pager's live page count (allocated
// minus free) does not grow with the cycle count. A leak here means
// makeLeafEntry/insertIntoTree/insertWithSplit failed to free an old overflow
// chain on update or on the merge-and-split replace path.
func TestBTreeReplaceDeleteInsertOverflowSequenceNoLeak(t *testing.T) {
	p := newTestPager(t)
	txID, err := p.BeginTx()
	if err != nil {
		t.Fatal(err)
	}
	bt, err := CreateBTree(p, txID)
	if err != nil {
		t.Fatal(err)
	}

	// Warm the tree with stable neighbours so the replaced key's leaf is
	// never the only leaf in the tree, matching a real table.
	for i := 0; i < 64; i++ {
		nk := []byte(fmt.Sprintf("neighbour/%04d", i))
		if err := bt.Insert(txID, nk, bytes.Repeat([]byte{byte(i)}, 300)); err != nil {
			t.Fatal(err)
		}
	}

	key := []byte("tile/replace-cycle")
	sizes := []int{50, 1_536, 1_537, 1_569, 2_500, 9_000, 50_000, 100}

	const cycles = 40
	for c := 0; c < cycles; c++ {
		size := sizes[c%len(sizes)]
		value := bytes.Repeat([]byte{byte(c)}, size)
		if err := bt.Insert(txID, key, value); err != nil {
			t.Fatalf("cycle %d insert size %d: %v", c, size, err)
		}
		got, found, err := bt.Get(key)
		if err != nil || !found || !bytes.Equal(got, value) {
			t.Fatalf("cycle %d: get mismatch found=%v err=%v", c, found, err)
		}
	}
	if err := p.CommitTx(txID); err != nil {
		t.Fatal(err)
	}

	livePages := int(p.Superblock().PageCount) - p.freeMgr.Count()

	txID2, err := p.BeginTx()
	if err != nil {
		t.Fatal(err)
	}
	if ok, err := bt.Delete(txID2, key); err != nil || !ok {
		t.Fatalf("delete: ok=%v err=%v", ok, err)
	}
	var lastValue []byte
	for c := 0; c < cycles; c++ {
		size := sizes[c%len(sizes)]
		value := bytes.Repeat([]byte{byte(c + 1)}, size)
		if err := bt.Insert(txID2, key, value); err != nil {
			t.Fatalf("post-delete cycle %d insert size %d: %v", c, size, err)
		}
		lastValue = value
	}
	if err := p.CommitTx(txID2); err != nil {
		t.Fatal(err)
	}

	got, found, err := bt.Get(key)
	if err != nil || !found || !bytes.Equal(got, lastValue) {
		t.Fatalf("final value mismatch: found=%v err=%v", found, err)
	}
	for i := 0; i < 64; i++ {
		nk := []byte(fmt.Sprintf("neighbour/%04d", i))
		got, found, err := bt.Get(nk)
		if err != nil || !found || !bytes.Equal(got, bytes.Repeat([]byte{byte(i)}, 300)) {
			t.Fatalf("neighbour %d lost or corrupted: found=%v err=%v", i, found, err)
		}
	}

	livePagesAfter := int(p.Superblock().PageCount) - p.freeMgr.Count()
	// A handful of pages of slack covers legitimate structural change (e.g. a
	// split provoked incidentally along the way). A leak would instead track
	// the cycle count: 40 more overflow chains of up to ~13 pages each.
	if slack := livePagesAfter - livePages; slack > 20 {
		t.Fatalf("live pages grew by %d after %d replace/delete/insert cycles -- overflow pages are leaking", slack, cycles)
	}
}

// TestBTreeMultiLevelSplitInvariants forces at least one internal-level split
// (tree depth >= 3: root -> internal -> leaf, with the root itself having
// been rebuilt by a recursive insertIntoParent call), then checks search,
// insert, replace and range-scan all still hold, including after a durable
// close and read-only reopen. A small page size keeps the row count needed
// to reach that depth practical while exercising the identical split code
// used at the default page size.
func TestBTreeMultiLevelSplitInvariants(t *testing.T) {
	const rows = 20_000
	dir := t.TempDir()
	path := filepath.Join(dir, "multi.pages")
	p, err := OpenPager(PagerConfig{DBPath: path, PageSize: MinPageSize, MaxCachePages: 128})
	if err != nil {
		t.Fatal(err)
	}

	txID, err := p.BeginTx()
	if err != nil {
		t.Fatal(err)
	}
	bt, err := CreateBTree(p, txID)
	if err != nil {
		t.Fatal(err)
	}

	order := make([]int, rows)
	for i := range order {
		order[i] = i
	}
	rand.New(rand.NewSource(99)).Shuffle(rows, func(i, j int) { order[i], order[j] = order[j], order[i] })

	payloadSize := func(i int) int {
		if i%500 == 0 {
			return 9_000 + i%40_000 // occasional overflow chain in a deep tree
		}
		return 32 + (i*37)%860 // mostly small, to force many leaves cheaply
	}

	want := make(map[string][]byte, rows+1000)
	for _, i := range order {
		key := []byte(fmt.Sprintf("row/%08d", i))
		value := exactBoundaryPayload(i, payloadSize(i))
		want[string(key)] = value
		if err := bt.Insert(txID, key, value); err != nil {
			t.Fatalf("insert row %d: %v", i, err)
		}
	}
	root := bt.Root()

	depth := btreeDepth(t, p, root)
	if depth < 3 {
		t.Fatalf("tree depth = %d, want >= 3 (root -> internal -> leaf) to exercise recursive internal splits", depth)
	}

	// Insert, after the tree is already deep, more keys interleaved among the
	// existing ones (forcing further splits), and replace a sample of
	// existing keys with a different-sized value (crossing the inline/
	// overflow boundary in both directions).
	for i := rows; i < rows+1_000; i++ {
		key := []byte(fmt.Sprintf("row/%08d", i))
		value := exactBoundaryPayload(i, payloadSize(i))
		want[string(key)] = value
		if err := bt.Insert(txID, key, value); err != nil {
			t.Fatalf("post-depth insert row %d: %v", i, err)
		}
	}
	for i := 0; i < rows; i += rows / 200 {
		key := []byte(fmt.Sprintf("row/%08d", i))
		newSize := exactBoundaryPayloadSizes[(i/7)%len(exactBoundaryPayloadSizes)]
		value := exactBoundaryPayload(i+1, newSize)
		want[string(key)] = value
		if err := bt.Insert(txID, key, value); err != nil { // Insert replaces an existing key.
			t.Fatalf("replace row %d: %v", i, err)
		}
	}
	root = bt.Root()

	if err := p.CommitTx(txID); err != nil {
		t.Fatal(err)
	}

	// Search and range-scan on the still-open, writable tree.
	verifyFullScanAndGets(t, NewBTree(p, root), want)
	if err := verifyBTreePages(p, root); err != nil {
		t.Fatalf("page integrity before reopen: %v", err)
	}
	verifyLeafChainBothDirections(t, p, root, len(want))

	if err := p.Close(); err != nil {
		t.Fatal(err)
	}

	reader, err := OpenPager(PagerConfig{DBPath: path, PageSize: MinPageSize, MaxCachePages: 128, ReadOnly: true})
	if err != nil {
		t.Fatalf("read-only reopen: %v", err)
	}
	defer reader.Close()

	verifyFullScanAndGets(t, NewBTree(reader, root), want)
	if err := verifyBTreePages(reader, root); err != nil {
		t.Fatalf("page integrity after reopen: %v", err)
	}
	verifyLeafChainBothDirections(t, reader, root, len(want))

	// A bounded range scan over a middle slice must return exactly that
	// slice, in order, with nothing outside it.
	readTree := NewBTree(reader, root)
	lo, hi := []byte(fmt.Sprintf("row/%08d", 5_000)), []byte(fmt.Sprintf("row/%08d", 5_099))
	rangeSeen := 0
	if err := readTree.ScanRange(lo, hi, func(key, value []byte) bool {
		if bytes.Compare(key, lo) < 0 || bytes.Compare(key, hi) > 0 {
			t.Errorf("range scan returned out-of-range key %q", key)
		}
		expected, ok := want[string(key)]
		if !ok || !bytes.Equal(value, expected) {
			t.Errorf("range scan value mismatch for %q", key)
		}
		rangeSeen++
		return true
	}); err != nil {
		t.Fatal(err)
	}
	if rangeSeen != 100 {
		t.Fatalf("range scan [row/%08d, row/%08d] returned %d rows, want 100", 5_000, 5_099, rangeSeen)
	}

	if _, found, err := readTree.Get([]byte("row/absent!!")); err != nil || found {
		t.Fatalf("negative lookup found=%v err=%v, want not found", found, err)
	}
}

// ── Shared invariant helpers ─────────────────────────────────────────────

// verifyFullScanAndGets checks that a full ordered scan yields exactly want,
// and that every key in want is independently reachable via Get.
func verifyFullScanAndGets(t *testing.T, bt *BTree, want map[string][]byte) {
	t.Helper()
	seen := 0
	var prevKey []byte
	if err := bt.ScanRange(nil, nil, func(key, value []byte) bool {
		if prevKey != nil && bytes.Compare(prevKey, key) >= 0 {
			t.Errorf("scan order violated at %q after %q", key, prevKey)
		}
		prevKey = append(prevKey[:0], key...)
		expected, ok := want[string(key)]
		if !ok {
			t.Errorf("unexpected key %q", key)
			return true
		}
		if !bytes.Equal(value, expected) {
			t.Errorf("value mismatch for %q: got %d bytes, want %d", key, len(value), len(expected))
		}
		seen++
		return true
	}); err != nil {
		t.Fatal(err)
	}
	if seen != len(want) {
		t.Fatalf("scan returned %d rows, want %d", seen, len(want))
	}
	for k, v := range want {
		got, found, err := bt.Get([]byte(k))
		if err != nil || !found {
			t.Fatalf("get %q: found=%v err=%v", k, found, err)
		}
		if !bytes.Equal(got, v) {
			t.Fatalf("get %q returned different bytes", k)
		}
	}
}

// btreeDepth returns the number of pages from root to leaf, inclusive,
// following the leftmost child at every internal level.
func btreeDepth(t *testing.T, p *Pager, root PageID) int {
	t.Helper()
	depth := 1
	pid := root
	for {
		buf, err := p.ReadPage(pid)
		if err != nil {
			t.Fatal(err)
		}
		bp := WrapBTreePage(buf)
		if bp.IsLeaf() {
			p.UnpinPage(pid)
			return depth
		}
		child := bp.RightChild()
		if bp.slotCount() > 0 {
			child = bp.GetInternalEntry(0).ChildID
		}
		p.UnpinPage(pid)
		pid = child
		depth++
	}
}

// verifyBTreePages walks every page reachable from root exactly once and
// verifies its stored CRC, catching a page a split rewrote incorrectly (e.g.
// a corrupted slot directory) that a pure key/value comparison might miss if
// the corruption happens not to change the decoded bytes checked elsewhere.
func verifyBTreePages(p *Pager, root PageID) error {
	visited := make(map[PageID]struct{})
	var walk func(PageID) error
	walk = func(pid PageID) error {
		if _, ok := visited[pid]; ok {
			return fmt.Errorf("page %d visited more than once", pid)
		}
		visited[pid] = struct{}{}
		buf, err := p.ReadPage(pid)
		if err != nil {
			return err
		}
		if err := VerifyPageCRC(buf); err != nil {
			p.UnpinPage(pid)
			return fmt.Errorf("page %d: %w", pid, err)
		}
		bp := WrapBTreePage(buf)
		if bp.IsLeaf() {
			p.UnpinPage(pid)
			return nil
		}
		children := make([]PageID, 0, bp.slotCount()+1)
		for i := 0; i < bp.slotCount(); i++ {
			children = append(children, bp.GetInternalEntry(i).ChildID)
		}
		children = append(children, bp.RightChild())
		p.UnpinPage(pid)
		for _, child := range children {
			if err := walk(child); err != nil {
				return err
			}
		}
		return nil
	}
	return walk(root)
}

// verifyLeafChainBothDirections walks the leaf sibling chain forward from the
// leftmost leaf and backward from the rightmost leaf, checking both directions
// visit the same number of leaves and that prev/next pointers are mutually
// consistent -- a split that relinks siblings incorrectly (wrong NextLeaf on
// the reused left page, or a stale PrevLeaf on the old next-leaf) shows up
// here even when forward-only scans (used elsewhere) would not detect it.
func verifyLeafChainBothDirections(t *testing.T, p *Pager, root PageID, wantKeys int) {
	t.Helper()

	firstLeaf := func() PageID {
		pid := root
		for {
			buf, err := p.ReadPage(pid)
			if err != nil {
				t.Fatal(err)
			}
			bp := WrapBTreePage(buf)
			if bp.IsLeaf() {
				p.UnpinPage(pid)
				return pid
			}
			child := bp.RightChild()
			if bp.slotCount() > 0 {
				child = bp.GetInternalEntry(0).ChildID
			}
			p.UnpinPage(pid)
			pid = child
		}
	}()

	lastLeaf := func() PageID {
		pid := root
		for {
			buf, err := p.ReadPage(pid)
			if err != nil {
				t.Fatal(err)
			}
			bp := WrapBTreePage(buf)
			if bp.IsLeaf() {
				p.UnpinPage(pid)
				return pid
			}
			child := bp.RightChild()
			p.UnpinPage(pid)
			pid = child
		}
	}()

	forwardKeys, forwardLeaves := 0, 0
	for pid := firstLeaf; pid != InvalidPageID; {
		buf, err := p.ReadPage(pid)
		if err != nil {
			t.Fatal(err)
		}
		bp := WrapBTreePage(buf)
		forwardKeys += bp.KeyCount()
		forwardLeaves++
		next := bp.NextLeaf()
		p.UnpinPage(pid)
		pid = next
	}
	if forwardKeys != wantKeys {
		t.Fatalf("forward leaf-chain walk saw %d keys, want %d", forwardKeys, wantKeys)
	}

	backwardKeys, backwardLeaves := 0, 0
	for pid := lastLeaf; pid != InvalidPageID; {
		buf, err := p.ReadPage(pid)
		if err != nil {
			t.Fatal(err)
		}
		bp := WrapBTreePage(buf)
		backwardKeys += bp.KeyCount()
		backwardLeaves++
		prev := bp.PrevLeaf()
		p.UnpinPage(pid)
		pid = prev
	}
	if backwardKeys != wantKeys {
		t.Fatalf("backward leaf-chain walk saw %d keys, want %d", backwardKeys, wantKeys)
	}
	if forwardLeaves != backwardLeaves {
		t.Fatalf("forward walk visited %d leaves, backward walk visited %d", forwardLeaves, backwardLeaves)
	}
}
