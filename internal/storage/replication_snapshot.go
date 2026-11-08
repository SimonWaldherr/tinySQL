package storage

import (
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
)

const replicationSnapshotRows = 256

type replicationSnapshotHeader struct {
	Version int
	Tables  int
	Catalog diskCatalog
}

type replicationTableHeader struct {
	Table diskTable
	Rows  int
}

// WriteReplicationSnapshot writes a versioned, batched bootstrap image. Unlike
// SaveToWriter's historical whole-database GOB value, individual row batches
// bound the encoder/decoder's transient record buffers. The detached image is
// still proportional to the database; this is not a zero-copy snapshot.
func WriteReplicationSnapshot(ctx context.Context, db *DB, wal *AdvancedWAL, w io.Writer) (watermark, epoch uint64, err error) {
	dump, catalog, watermark, epoch, err := captureReplicationSnapshot(ctx, db, wal)
	if err != nil {
		return 0, 0, err
	}
	enc := gob.NewEncoder(w)
	if err := enc.Encode(replicationSnapshotHeader{Version: 1, Tables: len(dump), Catalog: catalog}); err != nil {
		return 0, 0, err
	}
	for i := range dump {
		if err := ctx.Err(); err != nil {
			return 0, 0, err
		}
		table := dump[i]
		rows := table.Rows
		table.Rows = nil
		dump[i] = diskTable{}
		if err := enc.Encode(replicationTableHeader{Table: table, Rows: len(rows)}); err != nil {
			return 0, 0, err
		}
		for start := 0; start < len(rows); start += replicationSnapshotRows {
			if err := ctx.Err(); err != nil {
				return 0, 0, err
			}
			end := min(start+replicationSnapshotRows, len(rows))
			if err := enc.Encode(rows[start:end]); err != nil {
				return 0, 0, err
			}
			clear(rows[start:end]) // encoded rows need not remain live until EOF
		}
	}
	if err := enc.Encode("tinySQL snapshot complete"); err != nil {
		return 0, 0, err
	}
	return watermark, epoch, nil
}

// Capture data and its resume position under the same lock order as Checkpoint.
// Release both locks before encoding or waiting on network flow control.
func captureReplicationSnapshot(ctx context.Context, db *DB, wal *AdvancedWAL) ([]diskTable, diskCatalog, uint64, uint64, error) {
	if db == nil || wal == nil {
		return nil, diskCatalog{}, 0, 0, errors.New("nil database or advanced WAL")
	}
	db.LockContentForRead()
	defer db.UnlockContentForRead()
	wal.mu.Lock()
	defer wal.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, diskCatalog{}, 0, 0, err
	}
	if wal.closed {
		return nil, diskCatalog{}, 0, 0, errors.New("advanced WAL is closed")
	}
	watermark := max(uint64(wal.committedLSN), uint64(wal.checkpointDataWatermark))
	db.mu.RLock()
	defer db.mu.RUnlock()
	count := 0
	for _, tenant := range db.tenants {
		count += len(tenant.tables)
	}
	dump := make([]diskTable, 0, count)
	for name, tenant := range db.tenants {
		for _, table := range tenant.tables {
			if err := ctx.Err(); err != nil {
				return nil, diskCatalog{}, 0, 0, err
			}
			image := tableToDiskRangeIndexed(name, table, 0, 0, true)
			image.Rows = make([][]any, len(table.Rows))
			for i, row := range table.Rows {
				if i&255 == 0 {
					if err := ctx.Err(); err != nil {
						return nil, diskCatalog{}, 0, 0, err
					}
				}
				image.Rows[i] = diskRowFromTable(table, row)
				cloneMutableCells(image.Rows[i])
			}
			for j := range image.Cols {
				image.Cols[j].DefaultValue = cloneCell(image.Cols[j].DefaultValue)
			}
			dump = append(dump, image)
		}
	}
	return dump, catalogToDisk(db.Catalog()), watermark, wal.epoch, nil
}

// LoadReplicationSnapshot accepts only complete streamed images, including EOF
// after the terminal marker. It never publishes a partially received database.
// Existing files and SaveToWriter/LoadFromReader keep their historical format.
func LoadReplicationSnapshot(ctx context.Context, r io.Reader) (*DB, error) {
	dec := gob.NewDecoder(r)
	var header replicationSnapshotHeader
	if err := dec.Decode(&header); err != nil {
		return nil, err
	}
	if header.Version != 1 || header.Tables < 0 {
		return nil, fmt.Errorf("unsupported or invalid replication snapshot header")
	}
	db := NewDB()
	for range header.Tables {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		var next replicationTableHeader
		if err := dec.Decode(&next); err != nil {
			return nil, err
		}
		if next.Rows < 0 || next.Rows > int(^uint(0)>>1)/24 || len(next.Table.Rows) != 0 {
			return nil, errors.New("invalid replication table header")
		}
		var batches [][][]any
		for received := 0; received < next.Rows; {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			var batch [][]any // fresh decoding storage; prior batches are owned
			if err := dec.Decode(&batch); err != nil {
				return nil, err
			}
			if len(batch) == 0 || len(batch) > replicationSnapshotRows || len(batch) > next.Rows-received {
				return nil, errors.New("invalid replication row batch")
			}
			batches = append(batches, batch)
			received += len(batch)
		}
		// A row count alone must not allocate an arbitrarily large table on a
		// truncated stream. Allocate headers only after those rows arrived.
		rows := make([][]any, next.Rows)
		position := 0
		for _, batch := range batches {
			position += copy(rows[position:], batch)
		}
		next.Table.Rows = rows
		if err := db.Put(next.Table.Tenant, diskToTableOwned(next.Table)); err != nil {
			return nil, err
		}
	}
	var end string
	if err := dec.Decode(&end); err != nil {
		return nil, err
	}
	if end != "tinySQL snapshot complete" {
		return nil, errors.New("missing replication snapshot completion")
	}
	var extra any
	if err := dec.Decode(&extra); !errors.Is(err, io.EOF) {
		if err != nil {
			return nil, err
		}
		return nil, errors.New("unexpected trailing replication snapshot data")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	db.setCatalog(diskToCatalog(header.Catalog))
	return db, nil
}
