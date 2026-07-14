package pager

import (
	"encoding/binary"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
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
	// loads coordinates a cold read of one page. It is intentionally scoped to
	// a PageID, so unrelated cold pages still issue I/O in parallel while a
	// stampede for one tile/index page performs exactly one physical read.
	loads map[PageID]*pageLoad
	// transient tracks uncached read-only page buffers. A page in this map
	// must not be admitted until every caller that received its raw buffer has
	// unpinned it; UnpinPage identifies frames only by PageID.
	transient map[PageID]int
	// LRU doubly-linked list: head = most recent, tail = least recent.
	head *PageFrame
	tail *PageFrame
}

// pageLoad is published under PageBufferPool.mu and completed by closing done.
// Waiters never hold a mutex while waiting for disk I/O. transient means the
// result could not enter the bounded cache because all frames were pinned; the
// shared raw buffer remains valid until every waiter calls UnpinPage.
type pageLoad struct {
	done      chan struct{}
	buf       []byte
	err       error
	transient bool
}

func newPageBufferPool(maxPages int) *PageBufferPool {
	if maxPages <= 0 {
		maxPages = 1024
	}
	pool := &PageBufferPool{
		maxPages:  maxPages,
		pages:     make(map[PageID]*PageFrame, maxPages),
		loads:     make(map[PageID]*pageLoad),
		transient: make(map[PageID]int),
	}
	return pool
}

func (bp *PageBufferPool) get(id PageID) (*PageFrame, bool) {
	f, ok := bp.pages[id]
	if ok {
		bp.moveToFront(f)
	}
	return f, ok
}

func (bp *PageBufferPool) put(f *PageFrame) {
	if existing, exists := bp.pages[f.id]; exists {
		bp.moveToFront(existing)
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

// putReadOnly admits one frame without ever exceeding maxPages. Callers
// must hold bp.mu. If all resident frames are pinned, it returns false:
// the caller may use its just-read buffer as query-local scratch but must
// not retain it in the cache. Waiting here can deadlock when a BLOB leaf is
// pinned while the same query needs an overflow page.
func (bp *PageBufferPool) putReadOnly(f *PageFrame) (*PageFrame, bool) {
	if bp.transient[f.id] > 0 {
		bp.transient[f.id]++
		return nil, false
	}
	if existing, exists := bp.pages[f.id]; exists {
		existing.pinned++
		bp.moveToFront(existing)
		return existing, true
	}
	if len(bp.pages) >= bp.maxPages && !bp.evictOne() {
		bp.transient[f.id]++
		return nil, false
	}
	bp.pages[f.id] = f
	bp.pushFront(f)
	return f, true
}

func (bp *PageBufferPool) unpin(id PageID) {
	if count := bp.transient[id]; count > 0 {
		if count == 1 {
			delete(bp.transient, id)
		} else {
			bp.transient[id] = count - 1
		}
		return
	}
	if f, ok := bp.get(id); ok && f.pinned > 0 {
		f.pinned--
	}
}

func (bp *PageBufferPool) remove(id PageID) {
	f, ok := bp.pages[id]
	if !ok {
		return
	}
	bp.unlink(f)
	delete(bp.pages, id)
}

// evictOne removes the least-recently-used clean, unpinned page.
// Returns false if no page can be evicted.
//
// A dirty page is the only in-memory copy of a committed WAL page until the
// next checkpoint. Dropping it would make a later B-Tree read fall back to an
// older/truncated main file. Read-only serving has no dirty pages, so its
// cache remains strictly bounded; mutable imports may temporarily exceed the
// target while a transaction is being built, but never lose durability.
func (bp *PageBufferPool) evictOne() bool {
	for f := bp.tail; f != nil; f = f.prev {
		if f.pinned == 0 && !f.dirty {
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
	ReadOnly      bool
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
	readOnly bool
	closed   bool

	pageReads   atomic.Int64
	cacheHits   atomic.Int64
	cacheMisses atomic.Int64
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
		if cfg.ReadOnly {
			return nil, fmt.Errorf("read-only pager requires existing database %q", cfg.DBPath)
		}
		isNew = true
	}

	flags := os.O_RDWR | os.O_CREATE
	if cfg.ReadOnly {
		flags = os.O_RDONLY
	}
	f, err := os.OpenFile(cfg.DBPath, flags, 0644)
	if err != nil {
		return nil, fmt.Errorf("open db file: %w", err)
	}

	p := &Pager{
		file:     f,
		pageSize: ps,
		path:     cfg.DBPath,
		walPath:  cfg.WALPath,
		readOnly: cfg.ReadOnly,
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

	// A published immutable artifact has no active WAL. Refuse a non-empty
	// sidecar instead of opening recovery in writable mode or silently serving
	// data whose committed state is ambiguous.
	walPath := cfg.WALPath
	if walPath == "" {
		walPath = cfg.DBPath + ".wal"
	}
	p.walPath = walPath
	if cfg.ReadOnly {
		if info, err := os.Stat(walPath); err == nil && info.Size() > WALFileHdrSize {
			f.Close()
			return nil, fmt.Errorf("read-only pager refuses active WAL %q", walPath)
		} else if err != nil && !os.IsNotExist(err) {
			f.Close()
			return nil, err
		}
		return p, nil
	}
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
		p.cacheHits.Add(1)
		return f.buf, nil
	}
	if loading := p.pool.loads[id]; loading != nil {
		p.pool.mu.Unlock()
		<-loading.done
		if loading.err != nil {
			return nil, loading.err
		}
		p.pool.mu.Lock()
		if loading.transient {
			// The leader already counted itself. Each waiter shares the same
			// query-local bytes and owns one matching transient pin.
			p.pool.transient[id]++
			buf := loading.buf
			p.pool.mu.Unlock()
			p.cacheHits.Add(1)
			return buf, nil
		}
		if f, ok := p.pool.get(id); ok {
			f.pinned++
			p.pool.mu.Unlock()
			p.cacheHits.Add(1)
			return f.buf, nil
		}
		// A completed loader normally leaves either a cache frame or a
		// transient result. Retrying is safer than returning bytes whose
		// ownership cannot be proven if a future cache policy changes.
		p.pool.mu.Unlock()
		return p.readPageCached(id)
	}
	loading := &pageLoad{done: make(chan struct{})}
	p.pool.loads[id] = loading
	p.pool.mu.Unlock()

	// Cache miss — read from file.
	p.cacheMisses.Add(1)
	p.pageReads.Add(1)
	buf, err := p.readPageRaw(id)
	if err != nil {
		p.finishPageLoad(id, loading, nil, err, false)
		return nil, err
	}
	f := &PageFrame{id: id, buf: buf, pinned: 1}
	p.pool.mu.Lock()
	if p.readOnly {
		canonical, admitted := p.pool.putReadOnly(f)
		if !admitted {
			p.finishPageLoadLocked(id, loading, buf, nil, true)
			p.pool.mu.Unlock()
			return buf, nil
		}
		p.finishPageLoadLocked(id, loading, canonical.buf, nil, false)
		p.pool.mu.Unlock()
		return canonical.buf, nil
	}
	// Another reader may have fetched this page while we were in ReadAt.
	// Use that canonical frame; otherwise its pin count and the LRU links
	// would diverge from the bytes returned to the caller.
	if existing, ok := p.pool.get(id); ok {
		existing.pinned++
		p.finishPageLoadLocked(id, loading, existing.buf, nil, false)
		p.pool.mu.Unlock()
		return existing.buf, nil
	}
	p.pool.put(f)
	p.finishPageLoadLocked(id, loading, buf, nil, false)
	p.pool.mu.Unlock()
	return buf, nil
}

// finishPageLoad publishes the result of a cold read. It is split from the
// locked helper for the error path, where no buffer-pool mutation is pending.
func (p *Pager) finishPageLoad(id PageID, loading *pageLoad, buf []byte, err error, transient bool) {
	p.pool.mu.Lock()
	p.finishPageLoadLocked(id, loading, buf, err, transient)
	p.pool.mu.Unlock()
}

// finishPageLoadLocked must be called with p.pool.mu held. Channel close is
// the happens-before edge for all pageLoad fields observed by waiters.
func (p *Pager) finishPageLoadLocked(id PageID, loading *pageLoad, buf []byte, err error, transient bool) {
	loading.buf = buf
	loading.err = err
	loading.transient = transient
	delete(p.pool.loads, id)
	close(loading.done)
}

// UnpinPage decrements the pin count.
func (p *Pager) UnpinPage(id PageID) {
	p.pool.mu.Lock()
	defer p.pool.mu.Unlock()
	p.pool.unpin(id)
}

// WritePage writes (updates) a page through the WAL. The page image is
// logged to the WAL and cached as dirty. The caller should have called
// BeginTx beforehand.
func (p *Pager) WritePage(txID TxID, id PageID, buf []byte) error {
	if p.readOnly {
		return fmt.Errorf("pager is read-only")
	}
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
	if p.readOnly {
		return 0, fmt.Errorf("pager is read-only")
	}
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
	if p.readOnly {
		return InvalidPageID, nil
	}
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
	if p.readOnly {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
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
	if p.readOnly {
		return nil
	}
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
	if p.readOnly {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	fn(p.sb)
}

// PageSize returns the configured page size.
func (p *Pager) PageSize() int { return p.pageSize }

// PagerCacheStats reports the physical-read and bounded-cache state. The
// counters are deliberately process-local: they describe this open snapshot,
// not a persisted database property.
type PagerCacheStats struct {
	PageReads       int64
	CacheHits       int64
	CacheMisses     int64
	CachedPages     int
	PinnedPages     int
	TransientPages  int
	TransientFrames int
	MaxPages        int
}

func (p *Pager) CacheStats() PagerCacheStats {
	p.pool.mu.Lock()
	cached := len(p.pool.pages)
	pinned := 0
	for _, frame := range p.pool.pages {
		if frame.pinned > 0 {
			pinned++
		}
	}
	transientFrames := 0
	for _, count := range p.pool.transient {
		transientFrames += count
	}
	maxPages := p.pool.maxPages
	p.pool.mu.Unlock()
	return PagerCacheStats{
		PageReads:       p.pageReads.Load(),
		CacheHits:       p.cacheHits.Load(),
		CacheMisses:     p.cacheMisses.Load(),
		CachedPages:     cached,
		PinnedPages:     pinned,
		TransientPages:  len(p.pool.transient),
		TransientFrames: transientFrames,
		MaxPages:        maxPages,
	}
}

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
		if p.wal != nil {
			_ = p.wal.Close()
		}
		_ = p.file.Close()
		return err
	}
	if p.wal != nil {
		if err := p.wal.Close(); err != nil {
			_ = p.file.Close()
			return err
		}
	}
	return p.file.Close()
}

// Path returns the database file path.
func (p *Pager) Path() string { return p.path }

// WALPath returns the WAL file path.
func (p *Pager) WALPath() string { return p.walPath }
