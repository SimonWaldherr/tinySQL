package pager

import (
	"bytes"
	"testing"
)

func TestSlottedPageFailedUpdateKeepsOriginal(t *testing.T) {
	sp := InitSlottedPage(make([]byte, 64), PageTypeBTreeLeaf, 1)
	slot, err := sp.InsertRecord([]byte("original"))
	if err != nil {
		t.Fatal(err)
	}
	before := bytes.Clone(sp.Bytes())
	if err := sp.UpdateRecord(slot, bytes.Repeat([]byte{'x'}, 32)); err == nil {
		t.Fatal("expected page-full error")
	}
	if !bytes.Equal(sp.Bytes(), before) {
		t.Fatal("failed update modified the page")
	}
	if got := string(sp.GetRecord(slot)); got != "original" {
		t.Fatalf("original record lost after failed update: %q", got)
	}
}

func TestSlottedPageReuseSlotWithoutNewDirectoryEntry(t *testing.T) {
	sp := InitSlottedPage(make([]byte, 64), PageTypeBTreeLeaf, 1)
	slot, err := sp.InsertRecord([]byte("original"))
	if err != nil {
		t.Fatal(err)
	}
	if err := sp.DeleteRecord(slot); err != nil {
		t.Fatal(err)
	}
	// The record fits precisely when the existing directory slot is reused.
	data := bytes.Repeat([]byte{'x'}, sp.FreeSpace()+slotEntrySize)
	gotSlot, err := sp.InsertRecord(data)
	if err != nil {
		t.Fatalf("reusing slot: %v", err)
	}
	if gotSlot != slot || !bytes.Equal(sp.GetRecord(gotSlot), data) {
		t.Fatalf("reused slot %d, want %d and original data", gotSlot, slot)
	}
}

func TestSlottedPageMaxSizeAndEmptyRecord(t *testing.T) {
	sp := InitSlottedPage(make([]byte, MaxPageSize), PageTypeBTreeLeaf, 1)
	empty, err := sp.InsertRecord(nil)
	if err != nil || sp.IsDeleted(empty) || sp.GetRecord(empty) == nil {
		t.Fatalf("empty record on 64 KiB page: slot %d, error %v", empty, err)
	}
	slot, err := sp.InsertRecord([]byte("abc"))
	if err != nil {
		t.Fatal(err)
	}
	if got := string(sp.GetRecord(slot)); got != "abc" {
		t.Fatalf("record = %q", got)
	}
	if err := sp.UpdateRecord(slot, []byte("a")); err != nil {
		t.Fatal(err)
	}
	sp.Compact()
	if sp.IsDeleted(empty) || sp.GetRecord(empty) == nil {
		t.Fatal("compaction lost empty record")
	}
	if got := string(sp.GetRecord(slot)); got != "a" {
		t.Fatalf("record after update and compaction = %q", got)
	}
}
