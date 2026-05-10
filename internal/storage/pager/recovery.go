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
//   4. For each committed TX in LSN order, apply PAGE_IMAGE records
//      whose LSN > page's on-disk LSN.
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

// recoverApplyPages writes committed page images that are newer than the checkpoint.
func (p *Pager) recoverApplyPages(txMap map[TxID]*recoverTxEntry) (int, error) {
	applied := 0
	for _, tr := range txMap {
		if !tr.committed || tr.aborted {
			continue
		}
		for _, rec := range tr.pages {
			if rec.LSN <= LSN(p.sb.CheckpointLSN) {
				continue
			}
			if err := p.writePageRaw(rec.PageID, rec.Data); err != nil {
				return applied, fmt.Errorf("recover apply page %d: %w", rec.PageID, err)
			}
			applied++
		}
	}
	return applied, nil
}

// recoverFlushSuperblock updates the superblock after applying pages and syncs to disk.
func (p *Pager) recoverFlushSuperblock(txMap map[TxID]*recoverTxEntry, maxLSN LSN, maxTxID TxID) error {
	if err := p.file.Sync(); err != nil {
		return err
	}

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
		return nil
	}

	txMap, maxLSN, maxTxID := recoverClassifyWAL(records)

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
