package pager

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"path/filepath"
	"testing"
)

func TestPageBufferPoolLargeLimitRemainsDemandDriven(t *testing.T) {
	for _, limit := range []int{0, 4, 2048, 131072} {
		t.Run(fmt.Sprint(limit), func(t *testing.T) {
			pool := newPageBufferPool(limit)
			if limit == 0 {
				limit = 1024
			}
			if pool.maxPages != limit {
				t.Fatalf("cache limit = %d, want %d", pool.maxPages, limit)
			}
			// Grow beyond the initial map hint, while small limits must still
			// evict exactly the oldest clean, unpinned frame.
			count := min(limit+1, 2049)
			for i := 1; i <= count; i++ {
				pool.putReadOnly(&PageFrame{id: PageID(i)})
			}
			if len(pool.pages) != min(count, limit) {
				t.Fatalf("cached pages = %d, want %d", len(pool.pages), min(count, limit))
			}
			if _, oldest := pool.pages[1]; oldest != (count <= limit) {
				t.Fatal("cache growth changed LRU eviction")
			}
			if pool.head.id != PageID(count) || pool.tail.id != PageID(max(1, count-limit+1)) {
				t.Fatal("cache growth changed LRU order")
			}
		})
	}
}

func TestMarshalRowLargeVectorWireCompatibility(t *testing.T) {
	for _, dims := range []int{0, 768, 1536} {
		t.Run(fmt.Sprint(dims), func(t *testing.T) {
			vector := make([]float64, dims)
			for i := range vector {
				vector[i] = float64(i) * 0.001
			}
			if dims != 0 {
				copy(vector, []float64{math.Copysign(0, -1), math.Float64frombits(0x7ff8000000000042), math.Inf(1), math.SmallestNonzeroFloat64})
			}
			row := []any{float64(42), "doc", vector, []byte{1, 2}, nil}
			// Construct the published wire format independently of MarshalRow.
			want := binary.LittleEndian.AppendUint64([]byte{5, 0, 3}, math.Float64bits(42))
			want = append(want, 4, 3, 0, 'd', 'o', 'c', 6)
			want = binary.LittleEndian.AppendUint32(want, uint32(dims))
			for _, value := range vector {
				want = binary.LittleEndian.AppendUint64(want, math.Float64bits(value))
			}
			want = append(want, 5, 2, 0, 1, 2, 0)
			if got := MarshalRow(row, nil); !bytes.Equal(got, want) {
				t.Fatal("fresh-buffer encoding changed the stored row format")
			}
			buf := make([]byte, len(want), max(len(want), 2+len(row)*9))
			got := MarshalRow(row, buf)
			if &got[0] != &buf[0] || !bytes.Equal(got, want) {
				t.Fatal("sufficient caller buffer was not reused with identical output")
			}
			decoded, err := UnmarshalRow(want)
			if err != nil {
				t.Fatal(err)
			}
			for i, value := range decoded[2].([]float64) {
				if math.Float64bits(value) != math.Float64bits(vector[i]) {
					t.Fatalf("vector dimension %d changed bits", i)
				}
			}
			if decoded[0] != float64(42) || decoded[1] != "doc" || !bytes.Equal(decoded[3].([]byte), []byte{1, 2}) || decoded[4] != nil {
				t.Fatal("vector reservation changed adjacent columns")
			}
		})
	}
}

func TestGCPreservesSecondaryIndexTrees(t *testing.T) {
	path := filepath.Join(t.TempDir(), "indexed.db")
	pb, err := NewPageBackend(PageBackendConfig{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = pb.Close() })
	const count = 1500 // enough for an internal index node and overflow locator
	rows := make([][]any, count)
	unique := make([]IndexEntry, count)
	allIDs := make([]int, count)
	for i := range rows {
		rows[i] = []any{float64(i), "all"}
		unique[i] = IndexEntry{Key: RowKey(int64(i)), RowIDs: []int{i}}
		allIDs[i] = i
	}
	if err := pb.SaveTable("default", &TableData{
		Name: "indexed", Columns: []ColumnInfo{{Name: "id"}, {Name: "group", Type: 13}}, Rows: rows,
		Indexes: []IndexInfo{
			{Name: "by_id", Columns: []string{"id"}, Unique: true, Entries: unique},
			{Name: "by_group", Columns: []string{"group"}, Entries: []IndexEntry{{Key: []byte("all"), RowIDs: allIDs}}},
		},
	}); err != nil {
		t.Fatal(err)
	}
	result, err := pb.GC()
	if err != nil || result.Reclaimed != 0 || len(result.Errors) != 0 {
		t.Fatalf("GC of live indexed table = %+v, %v", result, err)
	}
	// Allocate another tree before reopening, so a live index mistakenly
	// added to the free list cannot merely appear intact in the page cache.
	saveTestTable(t, pb, "default", "later", count)
	if err := pb.Close(); err != nil {
		t.Fatal(err)
	}
	pb, err = NewPageBackend(PageBackendConfig{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	for _, i := range []int{0, count / 2, count - 1} {
		got, found, err := pb.LookupIndexRows("default", "indexed", "by_id", RowKey(int64(i)))
		if err != nil || !found || len(got) != 1 || got[0][0] != float64(i) {
			t.Fatalf("unique index after GC/reopen: row=%d got=%v found=%t err=%v", i, got, found, err)
		}
	}
	got, found, err := pb.LookupIndexRows("default", "indexed", "by_group", []byte("all"))
	if err != nil || !found || len(got) != count {
		t.Fatalf("nonunique overflow index after GC/reopen: rows=%d found=%t err=%v", len(got), found, err)
	}
}
