package pager

import (
	"fmt"
)

// ───────────────────────────────────────────────────────────────────────────
// Crash Recovery
// ───────────────────────────────────────────────────────────────────────────
//
// Recovery reads the WAL from the beginning and replays only fully
// committed transactions whose page images have an LSN > the page's
// current on-disk LSN (or the checkpoint LSN). Uncommitted/aborted
// transactions are discarded.
//
// Algorithm:
//   1. Read all WAL records.
//   2. Build a map TxID → list of PAGE_IMAGE records.
//   3. Track which TxIDs have a COMMIT record (committed set).
//   4. Apply the newest committed PAGE_IMAGE for each page whose LSN
//      exceeds the checkpoint LSN.
//   5. Fsync the database file.
//   6. Update and flush the superblock with new checkpoint_lsn.
//   7. Truncate the WAL.

// recoverTxEntry groups WAL records for a single transaction.
type recoverTxEntry struct {
	pages     []*WALRecord
	committed bool
	aborted   bool
}

// recoverClassifyWAL scans WAL records and groups them by TxID.
func recoverClassifyWAL(records []*WALRecord) (txMap map[TxID]*recoverTxEntry, maxLSN LSN, maxTxID TxID) {
	txMap = make(map[TxID]*recoverTxEntry)
	for _, rec := range records {
		if rec.LSN > maxLSN {
			maxLSN = rec.LSN
		}
		if rec.TxID > maxTxID {
			maxTxID = rec.TxID
		}
		switch rec.Type {
		case WALRecordBegin:
			txMap[rec.TxID] = &recoverTxEntry{}
		case WALRecordPageImage:
			tr, ok := txMap[rec.TxID]
			if !ok {
				tr = &recoverTxEntry{}
				txMap[rec.TxID] = tr
			}
			tr.pages = append(tr.pages, rec)
		case WALRecordCommit:
			if tr, ok := txMap[rec.TxID]; ok {
				tr.committed = true
			}
		case WALRecordAbort:
			if tr, ok := txMap[rec.TxID]; ok {
				tr.aborted = true
			}
		case WALRecordCheckpoint:
			// Checkpoint record; all prior transactions are flushed.
		}
	}
	return txMap, maxLSN, maxTxID
}

// recoverApplyPages writes the latest committed image of each page newer than
// the checkpoint. Full page images supersede earlier images independently of
// other pages, so replay needs only one write per changed page.
func (p *Pager) recoverApplyPages(txMap map[TxID]*recoverTxEntry) (int, error) {
	latest := make(map[PageID]*WALRecord)
	for _, tr := range txMap {
		if !tr.committed || tr.aborted {
			continue
		}
		for _, rec := range tr.pages {
			if rec.LSN <= LSN(p.sb.CheckpointLSN) {
				continue
			}
			// Go map iteration does not preserve WAL order. Applying each
			// transaction immediately could overwrite a newer committed page
			// with an older image from a transaction visited later.
			if previous := latest[rec.PageID]; previous == nil || rec.LSN > previous.LSN {
				latest[rec.PageID] = rec
			}
		}
	}
	applied := 0
	for pageID, rec := range latest {
		if err := p.writePageRaw(pageID, rec.Data); err != nil {
			return applied, fmt.Errorf("recover apply page %d: %w", pageID, err)
		}
		applied++
	}
	return applied, nil
}

// recoverFlushSuperblock updates the superblock after applying pages and syncs to disk.
func (p *Pager) recoverFlushSuperblock(txMap map[TxID]*recoverTxEntry, maxLSN LSN, maxTxID TxID) error {
	if err := p.file.Sync(); err != nil {
		return err
	}

	// NextTxID/NextPageID store the next ID to hand out, not the last one
	// used — hence the +1 below. maxTxID/rec.PageID are the highest IDs
	// actually observed in the replayed WAL, so the superblock's allocator
	// state must be bumped past them or a fresh transaction/page allocation
	// after recovery could collide with (and silently overwrite) one that
	// was just replayed.
	p.sb.CheckpointLSN = maxLSN
	if TxID(maxTxID+1) > p.sb.NextTxID {
		p.sb.NextTxID = TxID(maxTxID + 1)
	}

	for _, tr := range txMap {
		if !tr.committed {
			continue
		}
		for _, rec := range tr.pages {
			if PageID(rec.PageID+1) > p.sb.NextPageID {
				p.sb.NextPageID = PageID(rec.PageID + 1)
				p.sb.PageCount = uint64(p.sb.NextPageID)
			}
		}
	}

	sbBuf := MarshalSuperblock(p.sb, p.pageSize)
	if err := p.writePageRaw(0, sbBuf); err != nil {
		return fmt.Errorf("recover superblock: %w", err)
	}
	return p.file.Sync()
}

// Recover replays the WAL and applies committed transactions.
func (p *Pager) Recover() error {
	records, err := ReadAllRecords(p.walPath)
	if err != nil {
		return fmt.Errorf("recover read WAL: %w", err)
	}
	if len(records) == 0 {
		// Checkpoints truncate the WAL, but their LSN remains in the
		// superblock. New writes must advance past it after a clean reopen
		// or the next recovery would skip those writes as checkpointed.
		p.wal.SetNextLSN(LSN(p.sb.CheckpointLSN) + 1)
		return nil
	}

	txMap, maxLSN, maxTxID := recoverClassifyWAL(records)
	if checkpointLSN := LSN(p.sb.CheckpointLSN); maxLSN < checkpointLSN {
		// A crash after flushing the checkpoint but before truncating the
		// WAL may leave only older records. Never move the counter back.
		maxLSN = checkpointLSN
	}

	applied, err := p.recoverApplyPages(txMap)
	if err != nil {
		return err
	}

	if applied > 0 {
		if err := p.recoverFlushSuperblock(txMap, maxLSN, maxTxID); err != nil {
			return err
		}
	}

	p.wal.SetNextLSN(maxLSN + 1)
	return p.wal.Truncate()
}
