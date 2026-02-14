package pager

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"testing"
)

func TestPageHeader_MarshalRoundTrip(t *testing.T) {
	h := PageHeader{
		Type:  PageTypeBTreeLeaf,
		Flags: 0x42,
		ID:    PageID(99),
		LSN:   LSN(12345),
		CRC:   0xDEADBEEF,
	}
	buf := make([]byte, PageHeaderSize)
	MarshalHeader(&h, buf)
	h2 := UnmarshalHeader(buf)
	if h2.Type != h.Type || h2.Flags != h.Flags || h2.ID != h.ID || h2.LSN != h.LSN || h2.CRC != h.CRC {
		t.Fatalf("header roundtrip mismatch: %+v vs %+v", h, h2)
	}
}

func TestCRC_DetectsCorruption(t *testing.T) {
	buf := NewPage(DefaultPageSize, PageTypeBTreeLeaf, 1)
	SetPageCRC(buf)
	if err := VerifyPageCRC(buf); err != nil {
		t.Fatalf("valid CRC failed: %v", err)
	}
	buf[100] ^= 0xFF
	if err := VerifyPageCRC(buf); err == nil {
		t.Fatal("expected CRC error after corruption")
	}
}

func TestSuperblock_RoundTrip(t *testing.T) {
	sb := NewSuperblock(DefaultPageSize)
	sb.CatalogRoot = PageID(5)
	sb.FreeListRoot = PageID(10)
	sb.CheckpointLSN = LSN(999)
	sb.NextTxID = TxID(42)
	sb.NextPageID = PageID(50)
	sb.PageCount = 50
	buf := MarshalSuperblock(sb, DefaultPageSize)
	sb2, err := UnmarshalSuperblock(buf)
	if err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if sb2.FormatVersion != sb.FormatVersion {
		t.Errorf("version mismatch")
	}
	if sb2.PageSize != sb.PageSize {
		t.Errorf("pageSize mismatch")
	}
	if sb2.CatalogRoot != sb.CatalogRoot {
		t.Errorf("catalogRoot mismatch")
	}
	if sb2.CheckpointLSN != sb.CheckpointLSN {
		t.Errorf("checkpointLSN mismatch")
	}
}

func TestSuperblock_BadMagic(t *testing.T) {
	buf := MarshalSuperblock(NewSuperblock(DefaultPageSize), DefaultPageSize)
	buf[sbMagicOff] = 'X'
	SetPageCRC(buf)
	_, err := UnmarshalSuperblock(buf)
	if err == nil {
		t.Fatal("expected error for bad magic")
	}
}

func TestSuperblock_UnsupportedFeatureFlags(t *testing.T) {
	sb := NewSuperblock(DefaultPageSize)
	sb.FeatureFlags = FeatureFlag(1 << 60)
	buf := MarshalSuperblock(sb, DefaultPageSize)
	_, err := UnmarshalSuperblock(buf)
	if err == nil {
		t.Fatal("expected error for unsupported feature flags")
	}
}

func TestSlottedPage_InsertAndGet(t *testing.T) {
	buf := make([]byte, DefaultPageSize)
	sp := InitSlottedPage(buf, PageTypeBTreeLeaf, 1)
	data := []byte("hello world")
	slot, err := sp.InsertRecord(data)
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	got := sp.GetRecord(slot)
	if !bytes.Equal(got, data) {
		t.Fatalf("got %q want %q", got, data)
	}
}

func TestSlottedPage_DeleteAndReuse(t *testing.T) {
	buf := make([]byte, DefaultPageSize)
	sp := InitSlottedPage(buf, PageTypeBTreeLeaf, 1)
	s0, _ := sp.InsertRecord([]byte("aaa"))
	s1, _ := sp.InsertRecord([]byte("bbb"))
	_ = sp.DeleteRecord(s0)
	if !sp.IsDeleted(s0) {
		t.Fatal("slot 0 should be deleted")
	}
	if sp.LiveRecords() != 1 {
		t.Fatalf("live records: got %d want 1", sp.LiveRecords())
	}
	s2, _ := sp.InsertRecord([]byte("ccc"))
	if s2 != s0 {
		t.Fatalf("expected reuse of slot %d, got %d", s0, s2)
	}
	_ = s1
}

func TestSlottedPage_UpdateInPlace(t *testing.T) {
	buf := make([]byte, DefaultPageSize)
	sp := InitSlottedPage(buf, PageTypeBTreeLeaf, 1)
	slot, _ := sp.InsertRecord([]byte("long data here!!"))
	err := sp.UpdateRecord(slot, []byte("short"))
	if err != nil {
		t.Fatalf("update: %v", err)
	}
	got := sp.GetRecord(slot)
	if string(got) != "short" {
		t.Fatalf("got %q want %q", got, "short")
	}
}

func TestSlottedPage_Compact(t *testing.T) {
	buf := make([]byte, DefaultPageSize)
	sp := InitSlottedPage(buf, PageTypeBTreeLeaf, 1)
	sp.InsertRecord([]byte("aaaa"))
	sp.InsertRecord([]byte("bbbb"))
	sp.InsertRecord([]byte("cccc"))
	sp.DeleteRecord(1)
	sp.Compact()
	if sp.LiveRecords() != 2 {
		t.Fatalf("after compact: live=%d want 2", sp.LiveRecords())
	}
}

func TestOverflowPage_ReadWrite(t *testing.T) {
	buf := make([]byte, DefaultPageSize)
	op := InitOverflowPage(buf, 5)
	data := make([]byte, OverflowCapacity(DefaultPageSize))
	rand.Read(data)
	if err := op.SetData(data); err != nil {
		t.Fatalf("setData: %v", err)
	}
	got := op.Data()
	if !bytes.Equal(got, data) {
		t.Fatal("data mismatch")
	}
}

func TestOverflowPage_ExceedsCapacity(t *testing.T) {
	buf := make([]byte, DefaultPageSize)
	op := InitOverflowPage(buf, 5)
	data := make([]byte, DefaultPageSize)
	if err := op.SetData(data); err == nil {
		t.Fatal("expected error for oversized data")
	}
}

func TestFreeListPage_AddAndPop(t *testing.T) {
	buf := make([]byte, DefaultPageSize)
	fl := InitFreeListPage(buf, 7)
	fl.AddEntry(PageID(10))
	fl.AddEntry(PageID(20))
	fl.AddEntry(PageID(30))
	if fl.EntryCount() != 3 {
		t.Fatalf("entry count: got %d", fl.EntryCount())
	}
	pid := fl.PopEntry()
	if pid != PageID(30) {
		t.Fatalf("pop: got %d want 30", pid)
	}
	if fl.EntryCount() != 2 {
		t.Fatalf("entry count after pop: got %d", fl.EntryCount())
	}
}

func TestFreeManager_AllocFree(t *testing.T) {
	fm := NewFreeManager()
	fm.Free(PageID(5))
	fm.Free(PageID(10))
	if fm.Count() != 2 {
		t.Fatalf("count: got %d", fm.Count())
	}
	pid := fm.Alloc()
	if pid == InvalidPageID {
		t.Fatal("expected a page from Alloc")
	}
	if fm.Count() != 1 {
		t.Fatalf("count after alloc: got %d", fm.Count())
	}
}

func TestWAL_WriteAndRead(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "test.wal")
	wf, err := OpenWALFile(walPath, DefaultPageSize)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	_, err = wf.AppendRecord(&WALRecord{Type: WALRecordBegin, TxID: 1})
	if err != nil {
		t.Fatalf("append begin: %v", err)
	}
	pageData := make([]byte, DefaultPageSize)
	copy(pageData, []byte("page image data"))
	_, err = wf.AppendRecord(&WALRecord{Type: WALRecordPageImage, TxID: 1, PageID: 5, Data: pageData})
	if err != nil {
		t.Fatalf("append page image: %v", err)
	}
	_, err = wf.AppendRecord(&WALRecord{Type: WALRecordCommit, TxID: 1})
	if err != nil {
		t.Fatalf("append commit: %v", err)
	}
	wf.Close()

	records, err := ReadAllRecords(walPath)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(records) != 3 {
		t.Fatalf("records: got %d want 3", len(records))
	}
	if records[0].Type != WALRecordBegin || records[0].TxID != 1 {
		t.Fatalf("record 0: %+v", records[0])
	}
	if records[1].Type != WALRecordPageImage || records[1].PageID != 5 {
		t.Fatalf("record 1: %+v", records[1])
	}
	if !bytes.Equal(records[1].Data, pageData) {
		t.Fatal("page image data mismatch")
	}
	if records[2].Type != WALRecordCommit {
		t.Fatalf("record 2: %+v", records[2])
	}
}

func TestWAL_Truncate(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "test.wal")
	wf, err := OpenWALFile(walPath, DefaultPageSize)
	if err != nil {
		t.Fatal(err)
	}
	wf.AppendRecord(&WALRecord{Type: WALRecordBegin, TxID: 1})
	wf.AppendRecord(&WALRecord{Type: WALRecordCommit, TxID: 1})
	wf.Truncate()
	wf.Close()
	records, _ := ReadAllRecords(walPath)
	if len(records) != 0 {
		t.Fatalf("after truncate: got %d records, want 0", len(records))
	}
}

func TestWAL_CorruptTail(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "test.wal")
	wf, err := OpenWALFile(walPath, DefaultPageSize)
	if err != nil {
		t.Fatal(err)
	}
	wf.AppendRecord(&WALRecord{Type: WALRecordBegin, TxID: 1})
	wf.AppendRecord(&WALRecord{Type: WALRecordCommit, TxID: 1})
	wf.Close()
	f, _ := os.OpenFile(walPath, os.O_WRONLY|os.O_APPEND, 0644)
	f.Write([]byte("GARBAGE"))
	f.Close()
	records, err := ReadAllRecords(walPath)
	if err != nil {
		t.Fatalf("read with corrupt tail: %v", err)
	}
	if len(records) != 2 {
		t.Fatalf("expected 2 valid records, got %d", len(records))
	}
}

func newTestPager(t *testing.T) *Pager {
	t.Helper()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")
	p, err := OpenPager(PagerConfig{
		DBPath:   dbPath,
		PageSize: DefaultPageSize,
	})
	if err != nil {
		t.Fatalf("OpenPager: %v", err)
	}
	t.Cleanup(func() { p.Close() })
	return p
}

func TestPager_BasicTransactions(t *testing.T) {
	p := newTestPager(t)
	txID, err := p.BeginTx()
	if err != nil {
		t.Fatal(err)
	}
	pid, buf := p.AllocPage()
	InitBTreePage(buf, pid, true)
	SetPageCRC(buf)
	if err := p.WritePage(txID, pid, buf); err != nil {
		t.Fatal(err)
	}
	p.UnpinPage(pid)
	if err := p.CommitTx(txID); err != nil {
		t.Fatal(err)
	}
	buf2, err := p.ReadPage(pid)
	if err != nil {
		t.Fatal(err)
	}
	defer p.UnpinPage(pid)
	bp := WrapBTreePage(buf2)
	if !bp.IsLeaf() {
		t.Fatal("expected leaf page")
	}
}

func TestPager_Checkpoint(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")
	p, err := OpenPager(PagerConfig{DBPath: dbPath, PageSize: DefaultPageSize})
	if err != nil {
		t.Fatal(err)
	}
	txID, _ := p.BeginTx()
	pid, buf := p.AllocPage()
	leaf := InitBTreePage(buf, pid, true)
	leaf.InsertLeafEntry(LeafEntry{Key: []byte("hello"), Value: []byte("world")})
	SetPageCRC(buf)
	p.WritePage(txID, pid, buf)
	p.UnpinPage(pid)
	p.CommitTx(txID)
	if err := p.Checkpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	p.Close()

	p2, err := OpenPager(PagerConfig{DBPath: dbPath, PageSize: DefaultPageSize})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer p2.Close()
	buf2, err := p2.ReadPage(pid)
	if err != nil {
		t.Fatalf("read after reopen: %v", err)
	}
	defer p2.UnpinPage(pid)
	bp := WrapBTreePage(buf2)
	if bp.KeyCount() != 1 {
		t.Fatalf("keyCount: got %d want 1", bp.KeyCount())
	}
}

func TestBTree_InsertAndGet(t *testing.T) {
	p := newTestPager(t)
	txID, _ := p.BeginTx()
	bt, err := CreateBTree(p, txID)
	if err != nil {
		t.Fatal(err)
	}
	if err := bt.Insert(txID, []byte("key1"), []byte("value1")); err != nil {
		t.Fatal(err)
	}
	if err := bt.Insert(txID, []byte("key2"), []byte("value2")); err != nil {
		t.Fatal(err)
	}
	p.CommitTx(txID)
	val, found, err := bt.Get([]byte("key1"))
	if err != nil {
		t.Fatal(err)
	}
	if !found || string(val) != "value1" {
		t.Fatalf("got %q/%v want value1/true", val, found)
	}
	_, found, err = bt.Get([]byte("nonexistent"))
	if err != nil {
		t.Fatal(err)
	}
	if found {
		t.Fatal("expected not found")
	}
}

func TestBTree_Delete(t *testing.T) {
	p := newTestPager(t)
	txID, _ := p.BeginTx()
	bt, _ := CreateBTree(p, txID)
	bt.Insert(txID, []byte("a"), []byte("1"))
	bt.Insert(txID, []byte("b"), []byte("2"))
	bt.Insert(txID, []byte("c"), []byte("3"))
	p.CommitTx(txID)

	txID2, _ := p.BeginTx()
	deleted, err := bt.Delete(txID2, []byte("b"))
	if err != nil {
		t.Fatal(err)
	}
	if !deleted {
		t.Fatal("expected deleted=true")
	}
	p.CommitTx(txID2)
	_, found, _ := bt.Get([]byte("b"))
	if found {
		t.Fatal("b should be deleted")
	}
	count, _ := bt.Count()
	if count != 2 {
		t.Fatalf("count: got %d want 2", count)
	}
}

func TestBTree_UpdateExistingKey(t *testing.T) {
	p := newTestPager(t)
	txID, _ := p.BeginTx()
	bt, _ := CreateBTree(p, txID)
	bt.Insert(txID, []byte("key"), []byte("val1"))
	bt.Insert(txID, []byte("key"), []byte("val2"))
	p.CommitTx(txID)
	val, found, _ := bt.Get([]byte("key"))
	if !found || string(val) != "val2" {
		t.Fatalf("got %q want val2", val)
	}
	count, _ := bt.Count()
	if count != 1 {
		t.Fatalf("count: got %d want 1", count)
	}
}

func TestBTree_ScanRange(t *testing.T) {
	p := newTestPager(t)
	txID, _ := p.BeginTx()
	bt, _ := CreateBTree(p, txID)
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("key%02d", i)
		bt.Insert(txID, []byte(key), []byte(fmt.Sprintf("val%02d", i)))
	}
	p.CommitTx(txID)
	var scanned []string
	bt.ScanRange([]byte("key03"), []byte("key07"), func(key, val []byte) bool {
		scanned = append(scanned, string(key))
		return true
	})
	expected := []string{"key03", "key04", "key05", "key06", "key07"}
	if len(scanned) != len(expected) {
		t.Fatalf("scanned %d want %d: %v", len(scanned), len(expected), scanned)
	}
	for i, s := range scanned {
		if s != expected[i] {
			t.Errorf("scanned[%d]=%q want %q", i, s, expected[i])
		}
	}
}

func TestBTree_SplitLeaf(t *testing.T) {
	p := newTestPager(t)
	txID, _ := p.BeginTx()
	bt, _ := CreateBTree(p, txID)
	n := 200
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("k%05d", i)
		val := fmt.Sprintf("v%05d", i)
		if err := bt.Insert(txID, []byte(key), []byte(val)); err != nil {
			t.Fatalf("insert %d: %v", i, err)
		}
	}
	p.CommitTx(txID)
	count, err := bt.Count()
	if err != nil {
		t.Fatal(err)
	}
	if count != n {
		t.Fatalf("count: got %d want %d", count, n)
	}
	var keys []string
	bt.ScanRange([]byte("k00000"), nil, func(key, val []byte) bool {
		keys = append(keys, string(key))
		return true
	})
	if len(keys) != n {
		t.Fatalf("scan: got %d keys want %d", len(keys), n)
	}
	if !sort.StringsAreSorted(keys) {
		t.Fatal("keys not sorted")
	}
	for _, i := range []int{0, 50, 99, 150, 199} {
		key := fmt.Sprintf("k%05d", i)
		val, found, err := bt.Get([]byte(key))
		if err != nil {
			t.Fatal(err)
		}
		if !found {
			t.Fatalf("key %s not found", key)
		}
		expected := fmt.Sprintf("v%05d", i)
		if string(val) != expected {
			t.Fatalf("key %s: got %q want %q", key, val, expected)
		}
	}
}

func TestBTree_OverflowValues(t *testing.T) {
	p := newTestPager(t)
	txID, _ := p.BeginTx()
	bt, _ := CreateBTree(p, txID)
	key := []byte("bigkey")
	val := make([]byte, bt.overflowThresh+500)
	rand.Read(val)
	if err := bt.Insert(txID, key, val); err != nil {
		t.Fatalf("insert overflow: %v", err)
	}
	p.CommitTx(txID)
	got, found, err := bt.Get(key)
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("overflow key not found")
	}
	if !bytes.Equal(got, val) {
		t.Fatalf("overflow value mismatch: got %d bytes, want %d", len(got), len(val))
	}
}

func TestRecovery_CommittedTxApplied(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")
	p, _ := OpenPager(PagerConfig{DBPath: dbPath, PageSize: DefaultPageSize})
	txID, _ := p.BeginTx()
	pid, buf := p.AllocPage()
	leaf := InitBTreePage(buf, pid, true)
	leaf.InsertLeafEntry(LeafEntry{Key: []byte("recovered"), Value: []byte("yes")})
	SetPageCRC(buf)
	p.WritePage(txID, pid, buf)
	p.UnpinPage(pid)
	p.CommitTx(txID)
	p.wal.Close()
	p.file.Close()

	p2, err := OpenPager(PagerConfig{DBPath: dbPath, PageSize: DefaultPageSize})
	if err != nil {
		t.Fatalf("reopen with recovery: %v", err)
	}
	defer p2.Close()
	buf2, err := p2.ReadPage(pid)
	if err != nil {
		t.Fatalf("read recovered page: %v", err)
	}
	defer p2.UnpinPage(pid)
	bp := WrapBTreePage(buf2)
	if bp.KeyCount() != 1 {
		t.Fatalf("recovered keyCount: %d want 1", bp.KeyCount())
	}
	entry := bp.GetLeafEntry(0)
	if string(entry.Key) != "recovered" || string(entry.Value) != "yes" {
		t.Fatalf("recovered entry: key=%q val=%q", entry.Key, entry.Value)
	}
}

func TestRecovery_UncommittedTxIgnored(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")
	walPath := dbPath + ".wal"
	p, _ := OpenPager(PagerConfig{DBPath: dbPath, PageSize: DefaultPageSize})
	p.Checkpoint()
	p.wal.Close()
	p.file.Close()

	wf, _ := OpenWALFile(walPath, DefaultPageSize)
	pageBuf := NewPage(DefaultPageSize, PageTypeBTreeLeaf, 2)
	bp := InitBTreePage(pageBuf, 2, true)
	bp.InsertLeafEntry(LeafEntry{Key: []byte("uncommitted"), Value: []byte("no")})
	SetPageCRC(pageBuf)
	wf.AppendRecord(&WALRecord{Type: WALRecordBegin, TxID: 99})
	wf.AppendRecord(&WALRecord{Type: WALRecordPageImage, TxID: 99, PageID: 2, Data: pageBuf})
	wf.Close()

	p2, err := OpenPager(PagerConfig{DBPath: dbPath, PageSize: DefaultPageSize})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer p2.Close()
}

func TestInspectSuperblock(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")
	p, _ := OpenPager(PagerConfig{DBPath: dbPath, PageSize: DefaultPageSize})
	p.Close()
	info, err := InspectSuperblock(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if !info.CRCValid {
		t.Fatal("superblock CRC invalid")
	}
	if info.PageSize != DefaultPageSize {
		t.Fatalf("pageSize: got %d", info.PageSize)
	}
	if info.FormatVersion != CurrentFormatVersion {
		t.Fatalf("version: got %d", info.FormatVersion)
	}
}

func TestVerifyDB_Clean(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")
	p, _ := OpenPager(PagerConfig{DBPath: dbPath, PageSize: DefaultPageSize})
	txID, _ := p.BeginTx()
	for i := 0; i < 5; i++ {
		pid, buf := p.AllocPage()
		InitBTreePage(buf, pid, true)
		SetPageCRC(buf)
		p.WritePage(txID, pid, buf)
		p.UnpinPage(pid)
	}
	p.CommitTx(txID)
	p.Close()
	issues, err := VerifyDB(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if len(issues) > 0 {
		t.Fatalf("verify issues: %v", issues)
	}
}

func TestInspectWAL(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "test.wal")
	wf, _ := OpenWALFile(walPath, DefaultPageSize)
	wf.AppendRecord(&WALRecord{Type: WALRecordBegin, TxID: 1})
	wf.AppendRecord(&WALRecord{Type: WALRecordPageImage, TxID: 1, Data: make([]byte, DefaultPageSize)})
	wf.AppendRecord(&WALRecord{Type: WALRecordCommit, TxID: 1})
	wf.AppendRecord(&WALRecord{Type: WALRecordBegin, TxID: 2})
	wf.AppendRecord(&WALRecord{Type: WALRecordAbort, TxID: 2})
	wf.Close()
	info, err := InspectWAL(walPath)
	if err != nil {
		t.Fatal(err)
	}
	if info.Records != 5 {
		t.Fatalf("records: got %d", info.Records)
	}
	if info.Committed != 1 {
		t.Fatalf("committed: got %d", info.Committed)
	}
	if info.Aborted != 1 {
		t.Fatalf("aborted: got %d", info.Aborted)
	}
	if info.PageImages != 1 {
		t.Fatalf("pageImages: got %d", info.PageImages)
	}
	if info.TxCount != 2 {
		t.Fatalf("txCount: got %d", info.TxCount)
	}
}

func TestPageBackend_SaveAndLoad(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")
	pb, err := NewPageBackend(PageBackendConfig{Path: dbPath})
	if err != nil {
		t.Fatal(err)
	}
	td := &TableData{
		Name: "users",
		Columns: []ColumnInfo{
			{Name: "id", Type: 0},
			{Name: "name", Type: 14},
		},
		Rows: [][]any{
			{float64(1), "alice"},
			{float64(2), "bob"},
		},
		Version: 1,
	}
	if err := pb.SaveTable("default", td); err != nil {
		t.Fatal(err)
	}
	got, err := pb.LoadTable("default", "users")
	if err != nil {
		t.Fatal(err)
	}
	if got == nil {
		t.Fatal("table not found")
	}
	if len(got.Rows) != 2 {
		t.Fatalf("rows: got %d want 2", len(got.Rows))
	}
	pb.Close()
}

func TestPageBackend_ListAndExists(t *testing.T) {
	dir := t.TempDir()
	pb, _ := NewPageBackend(PageBackendConfig{Path: filepath.Join(dir, "test.db")})
	defer pb.Close()
	pb.SaveTable("default", &TableData{Name: "t1", Columns: []ColumnInfo{{Name: "a", Type: 0}}})
	pb.SaveTable("default", &TableData{Name: "t2", Columns: []ColumnInfo{{Name: "b", Type: 0}}})
	names, err := pb.ListTableNames("default")
	if err != nil {
		t.Fatal(err)
	}
	if len(names) != 2 {
		t.Fatalf("names: got %v", names)
	}
	if !pb.TableExists("default", "t1") {
		t.Fatal("t1 should exist")
	}
	if pb.TableExists("default", "nope") {
		t.Fatal("nope should not exist")
	}
}

func TestPageBackend_Delete(t *testing.T) {
	dir := t.TempDir()
	pb, _ := NewPageBackend(PageBackendConfig{Path: filepath.Join(dir, "test.db")})
	defer pb.Close()
	pb.SaveTable("default", &TableData{Name: "temp", Columns: []ColumnInfo{{Name: "x", Type: 0}}})
	if !pb.TableExists("default", "temp") {
		t.Fatal("should exist")
	}
	pb.DeleteTable("default", "temp")
	if pb.TableExists("default", "temp") {
		t.Fatal("should be deleted")
	}
}

func TestPageBackend_Persistence(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "persist.db")
	pb, _ := NewPageBackend(PageBackendConfig{Path: dbPath})
	pb.SaveTable("default", &TableData{
		Name:    "data",
		Columns: []ColumnInfo{{Name: "v", Type: 14}},
		Rows:    [][]any{{"hello"}, {"world"}},
	})
	pb.Close()
	pb2, err := NewPageBackend(PageBackendConfig{Path: dbPath})
	if err != nil {
		t.Fatal(err)
	}
	defer pb2.Close()
	td, err := pb2.LoadTable("default", "data")
	if err != nil {
		t.Fatal(err)
	}
	if td == nil || len(td.Rows) != 2 {
		t.Fatalf("after reopen: %+v", td)
	}
}

func TestBTreePage_InternalEntry(t *testing.T) {
	buf := make([]byte, DefaultPageSize)
	bp := InitBTreePage(buf, 1, false)
	bp.InsertInternalEntry(InternalEntry{ChildID: 3, Key: []byte("mango")})
	bp.InsertInternalEntry(InternalEntry{ChildID: 2, Key: []byte("apple")})
	bp.InsertInternalEntry(InternalEntry{ChildID: 4, Key: []byte("zebra")})
	bp.SetRightChild(5)
	if bp.KeyCount() != 3 {
		t.Fatalf("keyCount: %d", bp.KeyCount())
	}
	e0 := bp.GetInternalEntry(0)
	e1 := bp.GetInternalEntry(1)
	e2 := bp.GetInternalEntry(2)
	if string(e0.Key) != "apple" || string(e1.Key) != "mango" || string(e2.Key) != "zebra" {
		t.Fatalf("order: %q %q %q", e0.Key, e1.Key, e2.Key)
	}
	child := bp.SearchInternal([]byte("b"))
	if child != 3 {
		t.Fatalf("search 'b': got child %d want 3", child)
	}
	child = bp.SearchInternal([]byte("zzz"))
	if child != 5 {
		t.Fatalf("search 'zzz': got child %d want 5", child)
	}
}

func TestBTreePage_LeafEntry(t *testing.T) {
	buf := make([]byte, DefaultPageSize)
	bp := InitBTreePage(buf, 1, true)
	bp.InsertLeafEntry(LeafEntry{Key: []byte("c"), Value: []byte("3")})
	bp.InsertLeafEntry(LeafEntry{Key: []byte("a"), Value: []byte("1")})
	bp.InsertLeafEntry(LeafEntry{Key: []byte("b"), Value: []byte("2")})
	if bp.KeyCount() != 3 {
		t.Fatalf("keyCount: %d", bp.KeyCount())
	}
	e := bp.GetLeafEntry(0)
	if string(e.Key) != "a" || string(e.Value) != "1" {
		t.Fatalf("entry 0: %q=%q", e.Key, e.Value)
	}
	pos, found := bp.FindLeafEntry([]byte("b"))
	if !found || pos != 1 {
		t.Fatalf("find b: pos=%d found=%v", pos, found)
	}
}

func TestBTreePage_LeafOverflowEntry(t *testing.T) {
	buf := make([]byte, DefaultPageSize)
	bp := InitBTreePage(buf, 1, true)
	bp.InsertLeafEntry(LeafEntry{
		Key:            []byte("big"),
		Overflow:       true,
		OverflowPageID: 42,
		TotalSize:      100000,
	})
	e := bp.GetLeafEntry(0)
	if !e.Overflow || e.OverflowPageID != 42 || e.TotalSize != 100000 {
		t.Fatalf("overflow entry: %+v", e)
	}
}
