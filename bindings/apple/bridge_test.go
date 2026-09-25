//go:build cgo

package main

import (
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func testDatabase(t *testing.T) uint64 {
	t.Helper()
	opened, err := openDatabase("")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = closeDatabase(opened.Handle) })
	return opened.Handle
}

func TestDatabaseRoundTripAndIsolation(t *testing.T) {
	id := testDatabase(t)
	for _, statement := range []string{"CREATE TABLE items (id INT, name TEXT, payload BLOB)", "CREATE TABLE empty_table (id INT)"} {
		if _, err := runSQL(id, statement, "[]", false); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := runSQL(id, "INSERT INTO items VALUES (?, ?, ?)", `[9223372036854775807,"Grüße ' 🦊",{"blob":"AAH/"}]`, false); err != nil {
		t.Fatal(err)
	}
	rows, err := runSQL(id, "SELECT id, name, payload FROM items WHERE id = ?", `[9223372036854775807]`, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows.Rows) != 1 || rows.Rows[0][0] != int64(math.MaxInt64) || rows.Rows[0][1] != "Grüße ' 🦊" {
		t.Fatalf("unexpected result: %#v", rows)
	}
	if rows.Rows[0][2].(map[string]string)["blob"] != "AAH/" {
		t.Fatal(rows.Rows)
	}
	other := testDatabase(t)
	if _, err := runSQL(other, "SELECT * FROM items", "[]", true); err == nil {
		t.Fatal("databases share state")
	}
	path := filepath.Join(t.TempDir(), "Grüße.snapshot")
	if _, err := saveDatabase(id, path); err != nil {
		t.Fatal(err)
	}
	restored, err := openDatabase(path)
	if err != nil {
		t.Fatal(err)
	}
	defer closeDatabase(restored.Handle)
	result, err := runSQL(restored.Handle, "SELECT * FROM items", "[]", true)
	if err != nil || len(result.Rows) != 1 {
		t.Fatalf("restore: %#v, %v", result, err)
	}
	if _, err := closeDatabase(id); err != nil {
		t.Fatal(err)
	}
	if _, err := runSQL(id, "SELECT 1", "[]", true); err == nil {
		t.Fatal("closed handle accepted")
	}
	if _, err := openDatabase(filepath.Join(t.TempDir(), "missing")); err == nil {
		t.Fatal("missing snapshot accepted")
	}
}

func TestParametersAndErrorEnvelope(t *testing.T) {
	for _, input := range []string{`[9223372036854775808]`, `[1e999]`, `[] []`, `[{}]`, `[[1]]`, `[{"blob":"!"}]`, `[{"real":"1"}]`, `[{"real":1e999}]`, `[{"real":1,"extra":2}]`, `{}`, `null`} {
		if _, err := decodeParameters(input); err == nil {
			t.Errorf("accepted %s", input)
		}
	}
	for _, fn := range []func() (response, error){
		func() (response, error) { panic("test panic") },
		func() (response, error) { return response{Rows: [][]any{{math.NaN()}}}, nil },
		func() (response, error) { return runSQL(0, "SELECT 1", "[]", true) },
	} {
		var result response
		if err := json.Unmarshal(encodeResponse(fn), &result); err != nil || result.Error == "" {
			t.Fatalf("missing JSON error: %#v, %v", result, err)
		}
	}
}

func TestRealParametersRetainType(t *testing.T) {
	values, err := decodeParameters(`[{"real":1},{"real":100000000000000000000},1]`)
	if err != nil {
		t.Fatal(err)
	}
	if values[0] != float64(1) || values[1] != float64(1e20) || values[2] != int64(1) {
		t.Fatalf("types or values changed: %#v", values)
	}
}

func TestCorruptGzipSnapshotIsRejected(t *testing.T) {
	id := testDatabase(t)
	path := filepath.Join(t.TempDir(), "database.gz")
	if _, err := saveDatabase(id, path); err != nil {
		t.Fatal(err)
	}
	valid, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"checksum", "truncated"} {
		t.Run(name, func(t *testing.T) {
			data := append([]byte(nil), valid...)
			if name == "checksum" {
				data[len(data)-8] ^= 0xff
			} else {
				data = data[:len(data)-4]
			}
			if err := os.WriteFile(path, data, 0600); err != nil {
				t.Fatal(err)
			}
			if opened, err := openDatabase(path); err == nil {
				_, _ = closeDatabase(opened.Handle)
				t.Fatal("corrupt gzip snapshot accepted")
			}
		})
	}
}

func TestSnapshotsHaveNoImplicitPersistence(t *testing.T) {
	for _, name := range []string{"db.snapshot", "db.snapshot.gz"} {
		t.Run(name, func(t *testing.T) {
			id := testDatabase(t)
			for _, statement := range []string{"CREATE TABLE items (id INT)", "BEGIN", "INSERT INTO items VALUES (7)", "COMMIT"} {
				if _, err := runSQL(id, statement, "[]", false); err != nil {
					t.Fatal(err)
				}
			}
			path := filepath.Join(t.TempDir(), name)
			if _, err := saveDatabase(id, path); err != nil {
				t.Fatal(err)
			}
			loaded, err := openDatabase(path)
			if err != nil {
				t.Fatal(err)
			}
			defer closeDatabase(loaded.Handle)
			if _, err := os.Stat(path + ".wal"); !os.IsNotExist(err) {
				t.Fatalf("unexpected WAL: %v", err)
			}
			if _, err := runSQL(loaded.Handle, "INSERT INTO items VALUES (8)", "[]", false); err != nil {
				t.Fatal(err)
			}
			unchanged, err := openDatabase(path)
			if err != nil {
				t.Fatal(err)
			}
			defer closeDatabase(unchanged.Handle)
			rows, err := runSQL(unchanged.Handle, "SELECT id FROM items", "[]", true)
			if err != nil || len(rows.Rows) != 1 || rows.Rows[0][0] != int64(7) {
				t.Fatalf("snapshot changed: %#v, %v", rows, err)
			}
			if _, err := saveDatabase(loaded.Handle, path); err != nil {
				t.Fatal(err)
			}
		})
	}
	path := filepath.Join(t.TempDir(), "empty")
	if err := os.WriteFile(path, nil, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := openDatabase(path); err == nil {
		t.Fatal("empty snapshot accepted")
	}
}

func TestTransactionConnectionAndConcurrentClose(t *testing.T) {
	id := testDatabase(t)
	for _, statement := range []string{"CREATE TABLE items (id INT)", "BEGIN", "INSERT INTO items VALUES (1)", "ROLLBACK"} {
		if _, err := runSQL(id, statement, "[]", false); err != nil {
			t.Fatal(err)
		}
	}
	result, err := runSQL(id, "SELECT * FROM items", "[]", true)
	if err != nil || len(result.Rows) != 0 {
		t.Fatalf("rollback: %#v, %v", result, err)
	}
	var group sync.WaitGroup
	for i := 0; i < 16; i++ {
		group.Add(1)
		go func() { defer group.Done(); _, _ = runSQL(id, "SELECT * FROM items", "[]", true) }()
	}
	group.Add(1)
	go func() { defer group.Done(); _, _ = closeDatabase(id) }()
	group.Wait()
	if _, err := saveDatabase(id, filepath.Join(t.TempDir(), "closed")); err == nil {
		t.Fatal("saved closed database")
	}
}
