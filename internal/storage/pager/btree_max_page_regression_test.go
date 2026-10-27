package pager

import (
	"bytes"
	"fmt"
	"path/filepath"
	"testing"
)

func TestBTreeMaxPageSizeRecords(t *testing.T) {
	leaf := InitBTreePage(make([]byte, MaxPageSize), 1, true)
	if _, err := leaf.InsertLeafEntry(LeafEntry{Key: []byte("alpha"), Value: []byte("first value")}); err != nil {
		t.Fatal(err)
	}
	if got := leaf.GetLeafEntry(0); !bytes.Equal(got.Key, []byte("alpha")) || !bytes.Equal(got.Value, []byte("first value")) {
		t.Fatalf("first entry = %#v", got)
	}
	old := leaf.getSlotEntry(0)
	if err := leaf.UpdateLeafEntry(0, LeafEntry{Key: []byte("alpha"), Value: []byte("x")}); err != nil {
		t.Fatal(err)
	}
	if got := leaf.GetLeafEntry(0); !bytes.Equal(got.Value, []byte("x")) {
		t.Fatalf("updated entry = %#v", got)
	}
	newLength := leaf.getSlotEntry(0).Length
	for _, b := range leaf.buf[int(old.Offset)+int(newLength) : int(old.Offset)+int(old.Length)] {
		if b != 0 {
			t.Fatal("shortened record retained trailing bytes")
		}
	}
	internal := InitBTreePage(make([]byte, MaxPageSize), 2, false)
	if err := internal.InsertInternalEntry(InternalEntry{ChildID: 3, Key: []byte("middle")}); err != nil {
		t.Fatal(err)
	}
	if got := internal.GetInternalEntry(0); got.ChildID != 3 || !bytes.Equal(got.Key, []byte("middle")) {
		t.Fatalf("internal entry = %#v", got)
	}
}

func TestFindChildUsesSeparatorRanges(t *testing.T) {
	page := InitBTreePage(make([]byte, DefaultPageSize), 1, false)
	for i, key := range []string{"b", "d", "f"} {
		if err := page.InsertInternalEntry(InternalEntry{ChildID: PageID(i + 2), Key: []byte(key)}); err != nil {
			t.Fatal(err)
		}
	}
	page.SetRightChild(5)
	for _, tc := range []struct {
		key  string
		want PageID
	}{
		{"a", 2}, {"b", 3}, {"c", 3}, {"d", 4}, {"e", 4}, {"f", 5}, {"z", 5},
	} {
		if got := page.FindChild([]byte(tc.key)); got != tc.want {
			t.Errorf("FindChild(%q) = %d, want %d", tc.key, got, tc.want)
		}
	}
}

func TestBTreeMaxPageSizeRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "max-page.db")
	p, err := OpenPager(PagerConfig{DBPath: path, PageSize: MaxPageSize})
	if err != nil {
		t.Fatal(err)
	}
	tx, err := p.BeginTx()
	if err != nil {
		t.Fatal(err)
	}
	tree, err := CreateBTree(p, tx)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 160; i++ {
		key := []byte(fmt.Sprintf("key-%03d", i))
		value := bytes.Repeat([]byte{byte(i)}, 800)
		if err := tree.Insert(tx, key, value); err != nil {
			t.Fatalf("insert %d: %v", i, err)
		}
	}
	root := tree.Root()
	if err := p.CommitTx(tx); err != nil {
		t.Fatal(err)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	p, err = OpenPager(PagerConfig{DBPath: path, PageSize: MaxPageSize, ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	tree = NewBTree(p, root)
	for i := 0; i < 160; i++ {
		key := []byte(fmt.Sprintf("key-%03d", i))
		got, ok, err := tree.Get(key)
		if err != nil || !ok || !bytes.Equal(got, bytes.Repeat([]byte{byte(i)}, 800)) {
			t.Fatalf("get %d: found=%v err=%v length=%d", i, ok, err, len(got))
		}
	}
}
