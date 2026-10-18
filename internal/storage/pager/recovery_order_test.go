package pager

import (
	"fmt"
	"path/filepath"
	"testing"
)

func recoveryPageImage(t *testing.T, pageID PageID, value string) []byte {
	t.Helper()
	buf := NewPage(DefaultPageSize, PageTypeBTreeLeaf, pageID)
	leaf := InitBTreePage(buf, pageID, true)
	if _, err := leaf.InsertLeafEntry(LeafEntry{Key: []byte("key"), Value: []byte(value)}); err != nil {
		t.Fatal(err)
	}
	SetPageCRC(buf)
	return buf
}

func TestRecoveryLatestCommittedPageImage(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "recovery.db")
	p, err := OpenPager(PagerConfig{DBPath: dbPath})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = p.wal.Close()
		_ = p.file.Close()
	})
	pageID, _ := p.AllocPage()
	p.UnpinPage(pageID)
	const commits = 32
	for i := 0; i < commits; i++ {
		txID, err := p.BeginTx()
		if err != nil {
			t.Fatal(err)
		}
		if err := p.WritePage(txID, pageID, recoveryPageImage(t, pageID, fmt.Sprint(i))); err != nil {
			t.Fatal(err)
		}
		if err := p.CommitTx(txID); err != nil {
			t.Fatal(err)
		}
	}
	// Simulate loss of the cache without a checkpoint: every acknowledged
	// version of this page exists only in the WAL.
	if err := p.wal.Close(); err != nil {
		t.Fatal(err)
	}
	if err := p.file.Close(); err != nil {
		t.Fatal(err)
	}
	recovered, err := OpenPager(PagerConfig{DBPath: dbPath})
	if err != nil {
		t.Fatal(err)
	}
	defer recovered.Close()
	buf, err := recovered.ReadPage(pageID)
	if err != nil {
		t.Fatal(err)
	}
	defer recovered.UnpinPage(pageID)
	got := string(WrapBTreePage(buf).GetLeafEntry(0).Value)
	if want := fmt.Sprint(commits - 1); got != want {
		t.Fatalf("recovery restored %q, want latest committed value %q", got, want)
	}
}

func TestRecoveryCommitAfterCleanReopen(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "recovery.db")
	p, err := OpenPager(PagerConfig{DBPath: dbPath})
	if err != nil {
		t.Fatal(err)
	}
	pageID, _ := p.AllocPage()
	p.UnpinPage(pageID)
	write := func(value string) {
		t.Helper()
		txID, err := p.BeginTx()
		if err != nil {
			t.Fatal(err)
		}
		if err := p.WritePage(txID, pageID, recoveryPageImage(t, pageID, value)); err != nil {
			t.Fatal(err)
		}
		if err := p.CommitTx(txID); err != nil {
			t.Fatal(err)
		}
	}
	write("before checkpoint")
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	p, err = OpenPager(PagerConfig{DBPath: dbPath})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = p.wal.Close()
		_ = p.file.Close()
	})
	write("after checkpoint")
	// Leave only the WAL durable, as when a process exits before its next
	// checkpoint. New records must not reuse LSNs covered by the old one.
	if err := p.wal.Close(); err != nil {
		t.Fatal(err)
	}
	if err := p.file.Close(); err != nil {
		t.Fatal(err)
	}
	recovered, err := OpenPager(PagerConfig{DBPath: dbPath})
	if err != nil {
		t.Fatal(err)
	}
	defer recovered.Close()
	buf, err := recovered.ReadPage(pageID)
	if err != nil {
		t.Fatal(err)
	}
	defer recovered.UnpinPage(pageID)
	if got := string(WrapBTreePage(buf).GetLeafEntry(0).Value); got != "after checkpoint" {
		t.Fatalf("recovery restored %q, want acknowledged value after clean reopen", got)
	}
}

func TestRecoveryAppliesOnlyNewestEligibleImagePerPage(t *testing.T) {
	p, err := OpenPager(PagerConfig{DBPath: filepath.Join(t.TempDir(), "recovery.db")})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	p.sb.CheckpointLSN = 10
	image := func(lsn LSN, pageID PageID, value string) *WALRecord {
		return &WALRecord{Type: WALRecordPageImage, LSN: lsn, PageID: pageID, Data: recoveryPageImage(t, pageID, value)}
	}
	// Transaction IDs and commit order do not establish page-image order:
	// the highest eligible page LSN is authoritative, even if transactions
	// were interleaved or contain multiple images for the same page.
	txMap := map[TxID]*recoverTxEntry{
		100: {committed: true, pages: []*WALRecord{image(11, 1, "old"), image(30, 1, "latest")}},
		200: {committed: true, pages: []*WALRecord{image(20, 1, "middle"), image(21, 2, "other")}},
		300: {committed: true, aborted: true, pages: []*WALRecord{image(40, 1, "aborted")}},
		400: {pages: []*WALRecord{image(50, 1, "uncommitted")}},
		500: {committed: true, pages: []*WALRecord{image(10, 1, "checkpointed"), image(9, 3, "checkpointed")}},
	}
	applied, err := p.recoverApplyPages(txMap)
	if err != nil {
		t.Fatal(err)
	}
	if applied != 2 {
		t.Fatalf("wrote %d page images, want exactly one write for each of two changed pages", applied)
	}
	for pageID, want := range map[PageID]string{1: "latest", 2: "other"} {
		buf, err := p.readPageRaw(pageID)
		if err != nil {
			t.Fatal(err)
		}
		if got := string(WrapBTreePage(buf).GetLeafEntry(0).Value); got != want {
			t.Fatalf("page %d: got %q, want %q", pageID, got, want)
		}
	}
}
