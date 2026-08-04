package storage

import (
	"fmt"
	"path/filepath"
	"testing"
)

// benchWALCols is a representative row shape: an int id, a string name, and a
// float64 amount — the same scalar mix that dominates real WAL traffic and
// that hashWALValue/calculateChecksum now fast-path instead of going through
// fmt.Fprintf's reflection.
var benchWALCols = []Column{
	{Name: "id", Type: IntType},
	{Name: "name", Type: StringType},
	{Name: "amount", Type: FloatType},
}

func benchWALRow(i int) []any {
	return []any{int64(i), fmt.Sprintf("name-%d", i), float64(i) * 1.5}
}

// BenchmarkAdvancedWALCalculateChecksum isolates calculateChecksum from disk
// I/O, directly measuring the hashWALValue/column-descriptor fast paths that
// run on every logged column of every insert/update/delete.
func BenchmarkAdvancedWALCalculateChecksum(b *testing.B) {
	wal := &AdvancedWAL{}
	before := benchWALRow(1)
	after := benchWALRow(2)
	record := &WALRecord{
		LSN:         1,
		TxID:        1,
		OpType:      WALOpUpdate,
		Tenant:      "default",
		Table:       "bench",
		RowID:       1,
		BeforeImage: before,
		AfterImage:  after,
		Columns:     benchWALCols,
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = wal.calculateChecksum(record)
	}
}

// BenchmarkAdvancedWALLogInsert measures single-row insert-transaction
// logging throughput end to end (LogBegin+LogInsert+LogCommit), the
// dominant per-statement cost for autocommit writes.
func BenchmarkAdvancedWALLogInsert(b *testing.B) {
	walPath := filepath.Join(b.TempDir(), "bench.wal")
	wal, err := OpenAdvancedWAL(AdvancedWALConfig{Path: walPath, CheckpointEvery: 1 << 30})
	if err != nil {
		b.Fatalf("open wal: %v", err)
	}
	defer wal.Close()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		txID := wal.NewAutoTxID()
		if _, err := wal.LogBegin(txID); err != nil {
			b.Fatalf("log begin: %v", err)
		}
		if _, err := wal.LogInsert(txID, "default", "bench", int64(i), benchWALRow(i), benchWALCols); err != nil {
			b.Fatalf("log insert: %v", err)
		}
		if _, err := wal.LogCommit(txID); err != nil {
			b.Fatalf("log commit: %v", err)
		}
	}
}

// BenchmarkAdvancedWALRecoverManySmallTx replays a WAL of many small
// single-row insert transactions against a table that already holds a large
// number of rows and a secondary index — the scenario where Recover
// previously paid for a full RebuildSecondaryIndexes (O(rows log rows)) after
// every single replayed op instead of once for the whole replay.
func BenchmarkAdvancedWALRecoverManySmallTx(b *testing.B) {
	const baseRows = 4000
	const walTxCount = 300

	dir := b.TempDir()
	walPath := filepath.Join(dir, "recover_bench.wal")

	// Build the WAL file once: walTxCount single-row insert transactions.
	{
		wal, err := OpenAdvancedWAL(AdvancedWALConfig{Path: walPath, CheckpointEvery: 1 << 30})
		if err != nil {
			b.Fatalf("open wal: %v", err)
		}
		for i := 0; i < walTxCount; i++ {
			txID := wal.NewAutoTxID()
			rowID := int64(baseRows + i)
			if _, err := wal.LogBegin(txID); err != nil {
				b.Fatalf("log begin: %v", err)
			}
			if _, err := wal.LogInsert(txID, "default", "bench", rowID, benchWALRow(int(rowID)), benchWALCols); err != nil {
				b.Fatalf("log insert: %v", err)
			}
			if _, err := wal.LogCommit(txID); err != nil {
				b.Fatalf("log commit: %v", err)
			}
		}
		if err := wal.Close(); err != nil {
			b.Fatalf("close wal: %v", err)
		}
	}

	newBaseDB := func() *DB {
		db := NewDB()
		table := NewTable("bench", benchWALCols, false)
		for i := 0; i < baseRows; i++ {
			table.Rows = append(table.Rows, benchWALRow(i))
		}
		if err := table.CreateSecondaryIndex("idx_name", []string{"name"}, false); err != nil {
			b.Fatalf("create secondary index: %v", err)
		}
		if err := table.RebuildSecondaryIndexes(); err != nil {
			b.Fatalf("rebuild secondary indexes: %v", err)
		}
		if err := db.Put("default", table); err != nil {
			b.Fatalf("put table: %v", err)
		}
		return db
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		db := newBaseDB()
		wal, err := OpenAdvancedWAL(AdvancedWALConfig{Path: walPath, CheckpointEvery: 1 << 30})
		if err != nil {
			b.Fatalf("open wal: %v", err)
		}
		b.StartTimer()

		if _, err := wal.Recover(db); err != nil {
			b.Fatalf("recover: %v", err)
		}

		b.StopTimer()
		_ = wal.Close()
		b.StartTimer()
	}
}
