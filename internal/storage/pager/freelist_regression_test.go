package pager

import (
	"encoding/binary"
	"errors"
	"testing"
)

func TestFreeManagerLoadRejectsInvalidChainsWithoutChangingState(t *testing.T) {
	valid := make([]byte, 64)
	page := InitFreeListPage(valid, 1)
	page.AddEntry(7)
	cycle := bytesCopy(valid)
	WrapFreeListPage(cycle).SetNextFreeList(1)
	badCount := bytesCopy(valid)
	binary.LittleEndian.PutUint32(badCount[freeListCountOff:], ^uint32(0))
	missing := bytesCopy(valid)
	WrapFreeListPage(missing).SetNextFreeList(2)

	for name, data := range map[string][]byte{
		"cycle":            cycle,
		"invalid count":    badCount,
		"short page":       {1, 2},
		"later read error": missing,
	} {
		t.Run(name, func(t *testing.T) {
			fm := NewFreeManager()
			fm.Free(99)
			err := fm.LoadFromDisk(1, func(id PageID) ([]byte, error) {
				if id != 1 {
					return nil, errors.New("read failed")
				}
				return data, nil
			})
			if err == nil {
				t.Fatal("expected error")
			}
			if fm.Count() != 1 || fm.AllFree()[0] != 99 {
				t.Fatalf("state changed after failed load: %v", fm.AllFree())
			}
		})
	}
}

func TestFreeManagerLoadReplacesPreviousState(t *testing.T) {
	data := make([]byte, 64)
	InitFreeListPage(data, 1).AddEntry(7)
	fm := NewFreeManager()
	fm.Free(99)
	if err := fm.LoadFromDisk(1, func(PageID) ([]byte, error) { return data, nil }); err != nil {
		t.Fatal(err)
	}
	if fm.Count() != 1 || fm.Alloc() != 7 {
		t.Fatal("load did not replace the previous free set")
	}
}

func bytesCopy(src []byte) []byte { return append([]byte(nil), src...) }
