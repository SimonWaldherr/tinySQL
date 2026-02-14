package pager

import (
	"encoding/binary"
	"fmt"
	"os"
	"sync"
)

// ───────────────────────────────────────────────────────────────────────────
// Buffer Pool / Pager
// ───────────────────────────────────────────────────────────────────────────
//
// The Pager is the central I/O layer. It manages the database file, the WAL,
// the buffer pool (page cache with dirty tracking), the free-list, and the
// superblock. All page reads and writes go through the Pager so that CRC
// validation and WAL logging happen automatically.

// PageFrame is an in-memory cached page.
type PageFrame struct {
	id     PageID
	buf    []byte
	dirty  bool
	lsn    LSN // LSN of last modification
	pinned int // pin count (>0 = cannot evict)
	prev   *PageFrame
	next   *PageFrame
}

// BufferPoolConfig configures the page buffer pool.
type BufferPoolConfig struct {
	MaxPages int // maximum number of cached pages (default 1024)
}

// PageBufferPool is an LRU page cache with dirty-page tracking.
type PageBufferPool struct {
	mu       sync.Mutex
	maxPages int
	pages    map[PageID]*PageFrame
	// LRU doubly-linked list: head = most recent, tail = least recent.
	head *PageFrame
	tail *PageFrame
}

func newPageBufferPool(maxPages int) *PageBufferPool {
	if maxPages <= 0 {
		maxPages = 1024
	}
	return &PageBufferPool{
		maxPages: maxPages,
		pages:    make(map[PageID]*PageFrame, maxPages),
	}
}

func (bp *PageBufferPool) get(id PageID) (*PageFrame, bool) {
	f, ok := bp.pages[id]
	if ok {
		bp.moveToFront(f)
	}
	return f, ok
}

func (bp *PageBufferPool) put(f *PageFrame) {
	if _, exists := bp.pages[f.id]; exists {
		bp.moveToFront(f)
		return
	}
	// Evict if at capacity.
	for len(bp.pages) >= bp.maxPages {
		if !bp.evictOne() {
			break // all pages pinned — cannot evict
		}
	}
	bp.pages[f.id] = f
	bp.pushFront(f)
}

func (bp *PageBufferPool) remove(id PageID) {
	f, ok := bp.pages[id]
	if !ok {
		return
	}
	bp.unlink(f)
	delete(bp.pages, id)
}

// evictOne removes the least-recently-used unpinned page.
// Returns false if no page can be evicted.
func (bp *PageBufferPool) evictOne() bool {
	for f := bp.tail; f != nil; f = f.prev {
		if f.pinned == 0 {
			bp.unlink(f)
			delete(bp.pages, f.id)
			return true
		}
	}
	return false
}

// dirtyPages returns all dirty page frames.
func (bp *PageBufferPool) dirtyPages() []*PageFrame {
	var out []*PageFrame
	for _, f := range bp.pages {
		if f.dirty {
			out = append(out, f)
		}
	}
	return out
}

func (bp *PageBufferPool) pushFront(f *PageFrame) {
	f.prev = nil
	f.next = bp.head
	if bp.head != nil {
		bp.head.prev = f
	}
	bp.head = f
	if bp.tail == nil {
		bp.tail = f
	}
}

func (bp *PageBufferPool) unlink(f *PageFrame) {
	if f.prev != nil {
		f.prev.next = f.next
	} else {
		bp.head = f.next
	}
	if f.next != nil {
		f.next.prev = f.prev
	} else {
		bp.tail = f.prev
	}
	f.prev = nil
	f.next = nil
}

func (bp *PageBufferPool) moveToFront(f *PageFrame) {
	bp.unlink(f)
	bp.pushFront(f)
}

// ───────────────────────────────────────────────────────────────────────────
// Pager
// ───────────────────────────────────────────────────────────────────────────

// PagerConfig configures a Pager.
type PagerConfig struct {
	DBPath        string
	WALPath       string
	PageSize      int
	MaxCachePages int // buffer pool capacity (0 = default 1024)
}

// Pager manages page-level I/O, WAL, buffer pool, and free-list.
type Pager struct {
	mu       sync.RWMutex
	file     *os.File
	wal      *WALFile
	pool     *PageBufferPool
	sb       *Superblock
	freeMgr  *FreeManager
	pageSize int
	path     string
	walPath  string
	closed   bool
}

// OpenPager opens or creates a page-based database.
func OpenPager(cfg PagerConfig) (*Pager, error) {
	ps := cfg.PageSize
	if ps == 0 {
		ps = DefaultPageSize
	}
	if ps < MinPageSize || ps > MaxPageSize || ps&(ps-1) != 0 {
		return nil, fmt.Errorf("invalid page size %d", ps)
	}

	isNew := false
	if _, err := os.Stat(cfg.DBPath); os.IsNotExist(err) {
		isNew = true
	}

	f, err := os.OpenFile(cfg.DBPath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, fmt.Errorf("open db file: %w", err)
	}

	p := &Pager{
		file:     f,
		pageSize: ps,
		path:     cfg.DBPath,
		walPath:  cfg.WALPath,
		pool:     newPageBufferPool(cfg.MaxCachePages),
		freeMgr:  NewFreeManager(),
	}

	if isNew {
		sb := NewSuperblock(uint32(ps))
		buf := MarshalSuperblock(sb, ps)
		if _, err := f.WriteAt(buf, 0); err != nil {
			f.Close()
			return nil, fmt.Errorf("write superblock: %w", err)
		}
		if err := f.Sync(); err != nil {
			f.Close()
			return nil, err
		}
		p.sb = sb
	} else {
		sb, err := p.readSuperblock()
		if err != nil {
			f.Close()
			return nil, err
		}
		p.sb = sb
		p.pageSize = int(sb.PageSize) // honour on-disk page size

		// Load free list.
		if sb.FreeListRoot != InvalidPageID {
			if err := p.freeMgr.LoadFromDisk(sb.FreeListRoot, p.readPageRaw); err != nil {
				f.Close()
				return nil, fmt.Errorf("load freelist: %w", err)
			}
		}
	}

	// Open or create WAL.
	walPath := cfg.WALPath
	if walPath == "" {
		walPath = cfg.DBPath + ".wal"
	}
	p.walPath = walPath
	wf, err := OpenWALFile(walPath, p.pageSize)
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("open WAL file: %w", err)
	}
	p.wal = wf

	// If WAL has records, perform recovery before accepting new writes.
	if !isNew {
		if err := p.Recover(); err != nil {
			wf.Close()
			f.Close()
			return nil, fmt.Errorf("WAL recovery: %w", err)
		}
	}

	return p, nil
}

func (p *Pager) readSuperblock() (*Superblock, error) {
	buf := make([]byte, p.pageSize)
	if _, err := p.file.ReadAt(buf, 0); err != nil {
		return nil, fmt.Errorf("read superblock: %w", err)
	}
	return UnmarshalSuperblock(buf)
}

// readPageRaw reads a page directly from the database file (no cache).
func (p *Pager) readPageRaw(id PageID) ([]byte, error) {
	buf := make([]byte, p.pageSize)
	off := int64(id) * int64(p.pageSize)
	if _, err := p.file.ReadAt(buf, off); err != nil {
		return nil, fmt.Errorf("read page %d: %w", id, err)
	}
	if err := VerifyPageCRC(buf); err != nil {
		return nil, err
	}
	return buf, nil
}

// writePageRaw writes a page directly to the database file (no cache).
func (p *Pager) writePageRaw(id PageID, buf []byte) error {
	SetPageCRC(buf)
	off := int64(id) * int64(p.pageSize)
	if _, err := p.file.WriteAt(buf, off); err != nil {
		return fmt.Errorf("write page %d: %w", id, err)
	}
	return nil
}

// ── Public page I/O ───────────────────────────────────────────────────────

// ReadPage returns a page by ID, using the buffer pool cache.
// The page is pinned in the cache; call UnpinPage when done.
func (p *Pager) ReadPage(id PageID) ([]byte, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.readPageCached(id)
}

func (p *Pager) readPageCached(id PageID) ([]byte, error) {
	p.pool.mu.Lock()
	if f, ok := p.pool.get(id); ok {
		f.pinned++
		p.pool.mu.Unlock()
		return f.buf, nil
	}
	p.pool.mu.Unlock()

	// Cache miss — read from file.
	buf, err := p.readPageRaw(id)
	if err != nil {
		return nil, err
	}
	f := &PageFrame{id: id, buf: buf, pinned: 1}
	p.pool.mu.Lock()
	p.pool.put(f)
	p.pool.mu.Unlock()
	return buf, nil
}

// UnpinPage decrements the pin count.
func (p *Pager) UnpinPage(id PageID) {
	p.pool.mu.Lock()
	defer p.pool.mu.Unlock()
	if f, ok := p.pool.get(id); ok && f.pinned > 0 {
		f.pinned--
	}
}

// WritePage writes (updates) a page through the WAL. The page image is
// logged to the WAL and cached as dirty. The caller should have called
// BeginTx beforehand.
func (p *Pager) WritePage(txID TxID, id PageID, buf []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	// NOTE: CRC is set by the caller (BTree layer).  We skip re-computing
	// it here to avoid redundant work.

	// Log full page image to WAL.
	rec := &WALRecord{
		Type:   WALRecordPageImage,
		TxID:   txID,
		PageID: id,
		Data:   append([]byte{}, buf...), // copy
	}
	lsn, err := p.wal.AppendRecord(rec)
	if err != nil {
		return fmt.Errorf("WAL write page %d: %w", id, err)
	}

	// Update buffer pool.
	p.pool.mu.Lock()
	f, ok := p.pool.get(id)
	if !ok {
		f = &PageFrame{id: id, buf: make([]byte, p.pageSize)}
		p.pool.put(f)
	}
	copy(f.buf, buf)
	f.dirty = true
	f.lsn = lsn
	p.pool.mu.Unlock()

	return nil
}

// ── Transaction management ────────────────────────────────────────────────

// BeginTx starts a new transaction and writes a BEGIN record to the WAL.
func (p *Pager) BeginTx() (TxID, error) {
	p.mu.Lock()
	txID := p.sb.NextTxID
	p.sb.NextTxID++
	p.mu.Unlock()

	rec := &WALRecord{Type: WALRecordBegin, TxID: txID}
	if _, err := p.wal.AppendRecord(rec); err != nil {
		return 0, err
	}
	return txID, nil
}

// CommitTx writes a COMMIT record and fsyncs the WAL.
func (p *Pager) CommitTx(txID TxID) error {
	rec := &WALRecord{Type: WALRecordCommit, TxID: txID}
	if _, err := p.wal.AppendRecord(rec); err != nil {
		return err
	}
	return p.wal.Sync()
}

// AbortTx writes an ABORT record. Dirty pages for this TX will be
// discarded on the next recovery or checkpoint.
func (p *Pager) AbortTx(txID TxID) error {
	rec := &WALRecord{Type: WALRecordAbort, TxID: txID}
	_, err := p.wal.AppendRecord(rec)
	return err
}

// ── Page allocation ───────────────────────────────────────────────────────

// AllocPage allocates a new page (from the free-list or by extending the file).
// Returns the page ID and a zeroed buffer. The page is pinned in the cache.
func (p *Pager) AllocPage() (PageID, []byte) {
	p.mu.Lock()
	defer p.mu.Unlock()

	pid := p.freeMgr.Alloc()
	if pid == InvalidPageID {
		pid = p.sb.NextPageID
		p.sb.NextPageID++
		p.sb.PageCount++
	}
	buf := make([]byte, p.pageSize)
	// Put in pool pinned.
	f := &PageFrame{id: pid, buf: buf, pinned: 1}
	p.pool.mu.Lock()
	p.pool.put(f)
	p.pool.mu.Unlock()
	return pid, buf
}

// FreePage marks a page as free for reuse.
func (p *Pager) FreePage(pid PageID) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.freeMgr.Free(pid)
	p.pool.mu.Lock()
	p.pool.remove(pid)
	p.pool.mu.Unlock()
}

// freePageLocked is like FreePage but assumes p.mu is already held.
func (p *Pager) freePageLocked(pid PageID) {
	p.freeMgr.Free(pid)
	p.pool.mu.Lock()
	p.pool.remove(pid)
	p.pool.mu.Unlock()
}

// freeOldFreeListChain walks the old free-list chain and adds those pages
// to the FreeManager so they can be reused. Must be called with p.mu held.
func (p *Pager) freeOldFreeListChain(head PageID) {
	pid := head
	for pid != InvalidPageID {
		buf, err := p.readPageRaw(pid)
		if err != nil {
			break
		}
		fl := WrapFreeListPage(buf)
		next := fl.NextFreeList()
		p.freeMgr.Free(pid)
		pid = next
	}
}

// ── Checkpoint ────────────────────────────────────────────────────────────

// Checkpoint flushes all dirty pages to the database file, writes an updated
// superblock, fsyncs the file, then truncates the WAL.
func (p *Pager) Checkpoint() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Write checkpoint record to WAL.
	rec := &WALRecord{Type: WALRecordCheckpoint}
	lsn, err := p.wal.AppendRecord(rec)
	if err != nil {
		return err
	}
	if err := p.wal.Sync(); err != nil {
		return err
	}

	// Flush dirty pages to main file.
	p.pool.mu.Lock()
	dirty := p.pool.dirtyPages()
	for _, f := range dirty {
		SetPageCRC(f.buf)
		if err := p.writePageRaw(f.id, f.buf); err != nil {
			p.pool.mu.Unlock()
			return fmt.Errorf("checkpoint flush page %d: %w", f.id, err)
		}
		f.dirty = false
	}
	p.pool.mu.Unlock()

	// Free old free-list chain pages before writing the new one.
	oldFLHead := p.sb.FreeListRoot
	if oldFLHead != InvalidPageID {
		p.freeOldFreeListChain(oldFLHead)
	}

	// Flush free-list to disk.
	flHead, flPages := p.freeMgr.FlushToDisk(p.pageSize, func() (PageID, []byte) {
		pid := p.sb.NextPageID
		p.sb.NextPageID++
		p.sb.PageCount++
		return pid, make([]byte, p.pageSize)
	})
	for _, fb := range flPages {
		pid := PageID(binary.LittleEndian.Uint32(fb[4:8]))
		if err := p.writePageRaw(pid, fb); err != nil {
			return fmt.Errorf("checkpoint freelist page: %w", err)
		}
	}

	// Update superblock.
	p.sb.FreeListRoot = flHead
	p.sb.CheckpointLSN = lsn
	sbBuf := MarshalSuperblock(p.sb, p.pageSize)
	if err := p.writePageRaw(0, sbBuf); err != nil {
		return fmt.Errorf("checkpoint superblock: %w", err)
	}

	// Fsync the main file.
	if err := p.file.Sync(); err != nil {
		return err
	}

	// Truncate WAL.
	return p.wal.Truncate()
}

// ── Superblock access ─────────────────────────────────────────────────────

// Superblock returns a copy of the current superblock.
func (p *Pager) Superblock() Superblock {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return *p.sb
}

// UpdateSuperblock updates the in-memory superblock fields. It does NOT
// write to disk. Use Checkpoint for that.
func (p *Pager) UpdateSuperblock(fn func(sb *Superblock)) {
	p.mu.Lock()
	defer p.mu.Unlock()
	fn(p.sb)
}

// PageSize returns the configured page size.
func (p *Pager) PageSize() int { return p.pageSize }

// ── Close ─────────────────────────────────────────────────────────────────

// Close performs a final checkpoint and closes all files.
func (p *Pager) Close() error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	p.mu.Unlock()

	// Final checkpoint to ensure all data is on disk.
	if err := p.Checkpoint(); err != nil {
		// Best effort — still close files.
		_ = p.wal.Close()
		_ = p.file.Close()
		return err
	}
	if err := p.wal.Close(); err != nil {
		_ = p.file.Close()
		return err
	}
	return p.file.Close()
}

// Path returns the database file path.
func (p *Pager) Path() string { return p.path }

// WALPath returns the WAL file path.
func (p *Pager) WALPath() string { return p.walPath }
