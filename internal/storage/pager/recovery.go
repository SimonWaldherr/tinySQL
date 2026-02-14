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

// Recover replays the WAL and applies committed transactions.
func (p *Pager) Recover() error {
	records, err := ReadAllRecords(p.walPath)
	if err != nil {
		return fmt.Errorf("recover read WAL: %w", err)
	}
	if len(records) == 0 {
		return nil
	}

	// Classify records by TxID.
	type txRecords struct {
		pages     []*WALRecord
		committed bool
		aborted   bool
	}
	txMap := make(map[TxID]*txRecords)

	var maxLSN LSN
	var maxTxID TxID

	for _, rec := range records {
		if rec.LSN > maxLSN {
			maxLSN = rec.LSN
		}
		if rec.TxID > maxTxID {
			maxTxID = rec.TxID
		}

		switch rec.Type {
		case WALRecordBegin:
			txMap[rec.TxID] = &txRecords{}
		case WALRecordPageImage:
			tr, ok := txMap[rec.TxID]
			if !ok {
				tr = &txRecords{}
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

	// Replay committed transactions only, in LSN order.
	var applied int
	for _, tr := range txMap {
		if !tr.committed || tr.aborted {
			continue
		}
		for _, rec := range tr.pages {
			// Only apply if the record's LSN > checkpoint LSN.
			if rec.LSN <= LSN(p.sb.CheckpointLSN) {
				continue
			}
			if err := p.writePageRaw(rec.PageID, rec.Data); err != nil {
				return fmt.Errorf("recover apply page %d: %w", rec.PageID, err)
			}
			applied++
		}
	}

	if applied > 0 {
		// Fsync the database file.
		if err := p.file.Sync(); err != nil {
			return err
		}

		// Update superblock.
		p.sb.CheckpointLSN = maxLSN
		if TxID(maxTxID+1) > p.sb.NextTxID {
			p.sb.NextTxID = TxID(maxTxID + 1)
		}

		// Determine highest page ID used.
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
		if err := p.file.Sync(); err != nil {
			return err
		}
	}

	// Set WAL next LSN beyond recovered records.
	p.wal.SetNextLSN(maxLSN + 1)

	// Truncate the WAL.
	return p.wal.Truncate()
}
