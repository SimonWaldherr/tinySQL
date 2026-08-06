package pager

import (
	"bytes"
	"fmt"
	"math/rand"
	"path/filepath"
	"sort"
	"testing"
)

// TestBTreeLargeRecordsByteBalancedSplits is a regression for a real MBTiles
// artifact that failed while rebuilding a leaf after a count-balanced split.
// The mixed 1.4–2.5 KiB payloads deliberately straddle the inline/overflow
// boundary; small and large values ensure both representations take part.
func TestBTreeLargeRecordsByteBalancedSplits(t *testing.T) {
	const rows = 10_240
	for _, orderName := range []string{"sorted", "random"} {
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
			if orderName == "random" {
				rand.New(rand.NewSource(26)).Shuffle(rows, func(i, j int) { order[i], order[j] = order[j], order[i] })
			}

			want := make(map[string][]byte, rows)
			for _, i := range order {
				key := []byte(fmt.Sprintf("tile/%08d", i))
				value := testTilePayload(i)
				want[string(key)] = value
				if err := bt.Insert(txID, key, value); err != nil {
					t.Fatalf("insert row %d: %v", i, err)
				}
			}
			root := bt.Root()
			if err := p.CommitTx(txID); err != nil {
				t.Fatal(err)
			}

			leaves, internals := countBTreePages(t, p, root)
			if leaves < 100 {
				t.Fatalf("expected many leaf splits, got %d leaves", leaves)
			}
			if internals < 3 {
				t.Fatalf("expected multiple internal splits, got %d internal pages", internals)
			}
			if err := p.Close(); err != nil {
				t.Fatal(err)
			}

			reader, err := OpenPager(PagerConfig{DBPath: path, PageSize: DefaultPageSize, MaxCachePages: 32, ReadOnly: true})
			if err != nil {
				t.Fatalf("read-only reopen: %v", err)
			}
			defer reader.Close()
			readTree := NewBTree(reader, root)
			seen := 0
			if err := readTree.ScanRange(nil, nil, func(key, value []byte) bool {
				expected, ok := want[string(key)]
				if !ok {
					t.Errorf("unexpected key %q", key)
					return false
				}
				if !bytes.Equal(value, expected) {
					t.Errorf("value mismatch for %q: got %d bytes, want %d", key, len(value), len(expected))
					return false
				}
				seen++
				return true
			}); err != nil {
				t.Fatal(err)
			}
			if seen != rows {
				t.Fatalf("scan returned %d rows, want %d", seen, rows)
			}

			for _, i := range []int{0, 1, 17, rows / 2, rows - 2, rows - 1} {
				key := []byte(fmt.Sprintf("tile/%08d", i))
				got, found, err := readTree.Get(key)
				if err != nil || !found {
					t.Fatalf("get %q: found=%v err=%v", key, found, err)
				}
				if !bytes.Equal(got, want[string(key)]) {
					t.Fatalf("get %q returned different bytes", key)
				}
			}
		})
	}
}

func TestBTreeOverflowDecisionUsesEncodedLeafRecordSize(t *testing.T) {
	p := newTestPager(t)
	txID, err := p.BeginTx()
	if err != nil {
		t.Fatal(err)
	}
	bt, err := CreateBTree(p, txID)
	if err != nil {
		t.Fatal(err)
	}

	// The value alone is below the preferred inline threshold. Together with
	// the long key, headers and slot entry it cannot fit an empty leaf, while
	// the overflow reference can. This used to fail with "btree page full".
	key := bytes.Repeat([]byte("k"), 6_700)
	value := bytes.Repeat([]byte("v"), 1_500)
	if err := bt.Insert(txID, key, value); err != nil {
		t.Fatalf("insert record that must overflow: %v", err)
	}
	if err := p.CommitTx(txID); err != nil {
		t.Fatal(err)
	}
	got, found, err := bt.Get(key)
	if err != nil || !found {
		t.Fatalf("get overflow record: found=%v err=%v", found, err)
	}
	if !bytes.Equal(got, value) {
		t.Fatal("overflow record did not round-trip")
	}
}

func TestBTreeGetReturnsCallerOwnedValues(t *testing.T) {
	p := newTestPager(t)
	txID, err := p.BeginTx()
	if err != nil {
		t.Fatal(err)
	}
	bt, err := CreateBTree(p, txID)
	if err != nil {
		t.Fatal(err)
	}
	values := map[string][]byte{
		"inline":   []byte("small value"),
		"overflow": bytes.Repeat([]byte{0xAB}, OverflowCapacity(DefaultPageSize)*3+17),
	}
	for key, value := range values {
		if err := bt.Insert(txID, []byte(key), value); err != nil {
			t.Fatalf("insert %s: %v", key, err)
		}
	}
	if err := p.CommitTx(txID); err != nil {
		t.Fatal(err)
	}
	for key, want := range values {
		present, err := bt.Contains([]byte(key))
		if err != nil || !present {
			t.Fatalf("contains %s: present=%v err=%v", key, present, err)
		}
		got, found, err := bt.Get([]byte(key))
		if err != nil || !found || !bytes.Equal(got, want) {
			t.Fatalf("first get %s: found=%v err=%v", key, found, err)
		}
		got[0] ^= 0xff
		again, found, err := bt.Get([]byte(key))
		if err != nil || !found || !bytes.Equal(again, want) {
			t.Fatalf("mutation escaped into %s storage: found=%v err=%v", key, found, err)
		}
	}
	if present, err := bt.Contains([]byte("missing")); err != nil || present {
		t.Fatalf("contains missing: present=%v err=%v", present, err)
	}
}

func TestBTreeColdContiguousOverflowLeavesTailUncached(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "overflow.pages")
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
	key := []byte("large-contiguous-value")
	capacity := OverflowCapacity(DefaultPageSize)
	value := make([]byte, capacity*40+137)
	for index := range value {
		value[index] = byte(index * 31)
	}
	if err := bt.Insert(txID, key, value); err != nil {
		t.Fatal(err)
	}
	root := bt.Root()
	if err := p.CommitTx(txID); err != nil {
		t.Fatal(err)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}

	overflowPages := (len(value) + capacity - 1) / capacity
	reader, err := OpenPager(PagerConfig{
		DBPath:        path,
		PageSize:      DefaultPageSize,
		MaxCachePages: overflowPages + 8,
		ReadOnly:      true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	readTree := NewBTree(reader, root)
	got, found, err := readTree.Get(key)
	if err != nil || !found {
		t.Fatalf("cold get: found=%v err=%v", found, err)
	}
	if !bytes.Equal(got, value) {
		t.Fatal("cold get returned different bytes")
	}
	first := reader.CacheStats()
	if first.CachedPages >= overflowPages {
		t.Fatalf("cold contiguous tail filled cache: cached=%d overflow-pages=%d", first.CachedPages, overflowPages)
	}

	got, found, err = readTree.Get(key)
	if err != nil || !found {
		t.Fatalf("warm get: found=%v err=%v", found, err)
	}
	if !bytes.Equal(got, value) {
		t.Fatal("warm get returned different bytes")
	}
	second := reader.CacheStats()
	if second.CachedPages <= first.CachedPages {
		t.Fatalf("warm get did not use normal cache path: before=%d after=%d", first.CachedPages, second.CachedPages)
	}
}

func testTilePayload(i int) []byte {
	var size int
	switch i % 8 {
	case 0:
		size = 48 // small payload
	case 7:
		size = 9_000 + i%997 // large, always overflow
	default:
		size = 1_400 + (i*113)%1_101 // 1,400 through 2,500 bytes
	}
	payload := make([]byte, size)
	for j := range payload {
		payload[j] = byte(i*31 + j*17)
	}
	return payload
}

func countBTreePages(t *testing.T, p *Pager, root PageID) (leaves, internals int) {
	t.Helper()
	visited := make(map[PageID]struct{})
	var walk func(PageID)
	walk = func(pid PageID) {
		if _, ok := visited[pid]; ok {
			t.Fatalf("B+Tree references page %d more than once", pid)
		}
		visited[pid] = struct{}{}
		buf, err := p.ReadPage(pid)
		if err != nil {
			t.Fatal(err)
		}
		bp := WrapBTreePage(buf)
		if bp.IsLeaf() {
			leaves++
			p.UnpinPage(pid)
			return
		}
		internals++
		children := make([]PageID, 0, bp.slotCount()+1)
		for i := 0; i < bp.slotCount(); i++ {
			children = append(children, bp.GetInternalEntry(i).ChildID)
		}
		children = append(children, bp.RightChild())
		p.UnpinPage(pid)
		for _, child := range children {
			walk(child)
		}
	}
	walk(root)
	return leaves, internals
}

func TestLeafSplitIndexBalancesEncodedBytes(t *testing.T) {
	bt := &BTree{pager: &Pager{pageSize: DefaultPageSize}}
	entries := []LeafEntry{
		{Key: []byte("a"), Value: make([]byte, 180)},
		{Key: []byte("b"), Value: make([]byte, 180)},
		{Key: []byte("c"), Value: make([]byte, 2_000)},
		{Key: []byte("d"), Value: make([]byte, 180)},
		{Key: []byte("e"), Value: make([]byte, 1_700)},
	}
	idx, err := bt.leafSplitIndex(entries)
	if err != nil {
		t.Fatal(err)
	}
	if idx == len(entries)/2 {
		t.Fatalf("split stayed count-balanced at %d", idx)
	}

	left := 0
	for _, entry := range entries[:idx] {
		left += leafRecordFootprint(entry)
	}
	right := 0
	for _, entry := range entries[idx:] {
		right += leafRecordFootprint(entry)
	}
	if left > btreeRecordCapacity(DefaultPageSize) || right > btreeRecordCapacity(DefaultPageSize) {
		t.Fatalf("invalid byte split: left=%d right=%d", left, right)
	}
	keys := make([]string, len(entries))
	for i, entry := range entries {
		keys[i] = string(entry.Key)
	}
	if !sort.StringsAreSorted(keys) {
		t.Fatal("test entries must stay sorted")
	}
}

func TestInternalSplitIndexBalancesEncodedBytes(t *testing.T) {
	bt := &BTree{pager: &Pager{pageSize: DefaultPageSize}}
	entries := []InternalEntry{
		{ChildID: 1, Key: bytes.Repeat([]byte("a"), 100)},
		{ChildID: 2, Key: bytes.Repeat([]byte("b"), 100)},
		{ChildID: 3, Key: bytes.Repeat([]byte("c"), 3_000)},
		{ChildID: 4, Key: bytes.Repeat([]byte("d"), 100)},
		{ChildID: 5, Key: bytes.Repeat([]byte("e"), 2_500)},
	}
	mid, err := bt.internalSplitIndex(entries)
	if err != nil {
		t.Fatal(err)
	}
	if mid == len(entries)/2 {
		t.Fatalf("internal split stayed count-balanced at %d", mid)
	}

	left, right := 0, 0
	for _, entry := range entries[:mid] {
		left += internalRecordFootprint(entry)
	}
	for _, entry := range entries[mid+1:] {
		right += internalRecordFootprint(entry)
	}
	if left > btreeRecordCapacity(DefaultPageSize) || right > btreeRecordCapacity(DefaultPageSize) {
		t.Fatalf("invalid internal byte split: left=%d right=%d", left, right)
	}
}
