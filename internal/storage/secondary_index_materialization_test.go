package storage

import (
	"bytes"
	"encoding/gob"
	"reflect"
	"testing"
)

func TestSecondaryIndexEncodeSnapshotIndependent(t *testing.T) {
	for _, warm := range []bool{false, true} {
		name := "cold"
		if warm {
			name = "warm"
		}
		t.Run(name, func(t *testing.T) {
			idx := &SecondaryIndex{
				Name: "idx", Columns: []string{"key"}, Unique: true,
				Entries: []IndexEntry{
					{Key: []byte("a"), RowIDs: []int{1}},
					{Key: []byte("b"), RowIDs: []int{2}},
				},
			}
			if warm {
				// Leave Entries stale so the snapshot must read the live list.
				idx.hydrate().Insert([]byte("a"), 3)
				idx.hydrate().Insert([]byte("c"), 4)
			}
			out := materializeSecondaryIndexesForEncode(map[string]*SecondaryIndex{"idx": idx, "nil": nil})
			snapshot := out["idx"]
			if len(out) != 1 || snapshot == idx || snapshot.fast != nil {
				t.Fatal("snapshot must contain an independent persisted representation")
			}
			if snapshot.Name != idx.Name || snapshot.Unique != idx.Unique || !reflect.DeepEqual(snapshot.Columns, idx.Columns) {
				t.Fatal("snapshot lost index metadata")
			}
			want := []IndexEntry{
				{Key: []byte("a"), RowIDs: []int{1}},
				{Key: []byte("b"), RowIDs: []int{2}},
			}
			if warm {
				want[0].RowIDs = []int{1, 3}
				want = append(want, IndexEntry{Key: []byte("c"), RowIDs: []int{4}})
			}
			if !reflect.DeepEqual(snapshot.Entries, want) {
				t.Fatalf("snapshot entries = %v, want %v", snapshot.Entries, want)
			}

			// Subsequent live mutations cannot change a pending snapshot.
			if warm {
				idx.fast.Remove([]byte("a"), 1)
				idx.fast.Insert([]byte("d"), 5)
			} else {
				idx.Entries[0].Key[0] = 'z'
				idx.Entries[0].RowIDs[0] = 99
			}
			idx.Columns[0] = "changed"
			if !reflect.DeepEqual(snapshot.Entries, want) || snapshot.Columns[0] != "key" {
				t.Fatal("source mutation changed the snapshot")
			}

			var buf bytes.Buffer
			if err := gob.NewEncoder(&buf).Encode(snapshot); err != nil {
				t.Fatal(err)
			}
			var decoded SecondaryIndex
			if err := gob.NewDecoder(&buf).Decode(&decoded); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(decoded.Entries, want) {
				t.Fatal("GOB round trip changed snapshot entries")
			}

			// The other direction must be independent too, including keys.
			snapshot.Entries[1].Key[0] = 'x'
			snapshot.Entries[1].RowIDs[0] = 100
			var rows []int
			if warm {
				rows, _ = idx.fast.Get([]byte("b"))
			} else {
				if string(idx.Entries[1].Key) != "b" {
					t.Fatal("snapshot mutation changed a source key")
				}
				rows = idx.Entries[1].RowIDs
			}
			if !reflect.DeepEqual(rows, []int{2}) {
				t.Fatal("snapshot mutation changed source row IDs")
			}
		})
	}
}
