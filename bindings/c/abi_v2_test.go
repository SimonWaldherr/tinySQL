//go:build cgo

package main

import (
	"encoding/base64"
	"encoding/json"
	"math"
	"path/filepath"
	"strings"
	"testing"
	"time"
	"unicode/utf8"
)

func TestInfoReportsVersionAndABI(t *testing.T) {
	var decoded struct {
		Version string `json:"version"`
		ABI     int    `json:"abi"`
	}
	if err := json.Unmarshal(encodeResponse(info), &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.Version == "" || decoded.ABI != abiVersion {
		t.Fatalf("info: %#v", decoded)
	}
}

func TestRunDetectsRowProducingStatements(t *testing.T) {
	id := testDatabase(t)
	created, err := runSQL(id, "CREATE TABLE t (id INT, name TEXT)", "", modeAuto)
	if err != nil || created.Columns != nil {
		t.Fatalf("create: %#v, %v", created, err)
	}
	inserted, err := runSQL(id, "/* seed */ INSERT INTO t VALUES (?, ?), (?, ?)", `[1,"a",2,"b"]`, modeAuto)
	if err != nil || inserted.RowsAffected != 2 {
		t.Fatalf("insert: %#v, %v", inserted, err)
	}
	selected, err := runSQL(id, "  -- comment\nSELECT id FROM t ORDER BY id", "", modeAuto)
	if err != nil || len(selected.Columns) != 1 || len(selected.Rows) != 2 {
		t.Fatalf("select: %#v, %v", selected, err)
	}
	returning, err := runSQL(id, "UPDATE t SET name = ? WHERE id = ? RETURNING id, name", `["z",2]`, modeAuto)
	if err != nil || len(returning.Rows) != 1 || returning.Rows[0][1] != "z" {
		t.Fatalf("returning: %#v, %v", returning, err)
	}
	updated, err := runSQL(id, "UPDATE t SET name = 'returning' WHERE id = 1", "", modeAuto)
	if err != nil || updated.RowsAffected != 1 || updated.Columns != nil {
		t.Fatalf("update with RETURNING text in a literal: %#v, %v", updated, err)
	}
	for query, want := range map[string]bool{
		"SELECT 1": true, "with x as (select 1) select * from x": true, "t": true, `"t"`: true,
		"PRAGMA table_info(t)": true, "EXPLAIN SELECT 1": true,
		"delete from t returning *": true, "DELETE FROM t -- returning": false,
		"BEGIN": false, "commit": false, "CREATE TABLE x (id INT)": false, "insert into t values (1, 'x')": false,
	} {
		if got := returnsRows(query); got != want {
			t.Errorf("returnsRows(%q) = %v, want %v", query, got, want)
		}
	}
}

func TestExecuteScriptAndTransactions(t *testing.T) {
	id := testDatabase(t)
	result, err := executeScript(id, `
		CREATE TABLE t (id INT, note TEXT);
		BEGIN;
		INSERT INTO t VALUES (1, 'a;b'), (2, 'c');
		COMMIT;
		CREATE TRIGGER audit AFTER INSERT ON t BEGIN UPDATE t SET note = 'seen' WHERE id = NEW.id; END;
		INSERT INTO t VALUES (3, 'x');`)
	if err != nil {
		t.Fatal(err)
	}
	if result.Statements != 6 || result.RowsAffected != 3 {
		t.Fatalf("script result: %#v", result)
	}
	rows, err := runSQL(id, "SELECT note FROM t ORDER BY id", "", modeQuery)
	if err != nil || len(rows.Rows) != 3 || rows.Rows[0][0] != "a;b" || rows.Rows[2][0] != "seen" {
		t.Fatalf("script rows: %#v, %v", rows, err)
	}
	if _, err := executeScript(id, "INSERT INTO t VALUES (4, 'y'); INSERT INTO nope VALUES (1)"); err == nil || !strings.Contains(err.Error(), "statement 2") {
		t.Fatalf("script error: %v", err)
	}
}

func TestExecuteBatchPreparesOnce(t *testing.T) {
	id := testDatabase(t)
	if _, err := runSQL(id, "CREATE TABLE b (id INT, payload BLOB, score FLOAT)", "", modeExec); err != nil {
		t.Fatal(err)
	}
	result, err := executeBatch(id, "INSERT INTO b VALUES (?, ?, ?)", `[[1,{"blob":"AA=="},{"real":1}],[2,null,2.5],[3,{"blob":""},null]]`)
	if err != nil || result.RowsAffected != 3 || result.Statements != 3 {
		t.Fatalf("batch: %#v, %v", result, err)
	}
	rows, err := runSQL(id, "SELECT id, payload, score FROM b ORDER BY id", "", modeQuery)
	if err != nil || len(rows.Rows) != 3 {
		t.Fatalf("rows: %#v, %v", rows, err)
	}
	if score, ok := rows.Rows[0][2].(float64); !ok || score != 1 {
		t.Fatalf("real parameter lost its type: %#v", rows.Rows[0][2])
	}
	for _, input := range []string{``, `[]  []`, `[1]`, `[[{}]]`, `null`, `{}`} {
		if _, err := executeBatch(id, "INSERT INTO b VALUES (?, ?, ?)", input); err == nil {
			t.Errorf("accepted batch %q", input)
		}
	}
	if _, err := executeBatch(id, "INSERT INTO b VALUES (?, ?, ?)", `[[4,null,null],[5,null]]`); err == nil || !strings.Contains(err.Error(), "row 1") {
		t.Fatalf("short row: %v", err)
	}
}

func TestOpenWithOptionsStorageModes(t *testing.T) {
	dir := t.TempDir()
	for _, mode := range []string{"disk", "json", "wal", "advanced_wal"} {
		t.Run(mode, func(t *testing.T) {
			options := `{"mode":"` + mode + `","path":` + jsonString(filepath.Join(dir, mode)) + `,"walSync":"normal"}`
			opened, err := openWithOptions(options)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := executeScript(opened.Handle, "CREATE TABLE t (id INT); INSERT INTO t VALUES (42)"); err != nil {
				t.Fatal(err)
			}
			if _, err := syncDatabase(opened.Handle); err != nil {
				t.Fatal(err)
			}
			if _, err := closeDatabase(opened.Handle); err != nil {
				t.Fatal(err)
			}
			reopened, err := openWithOptions(options)
			if err != nil {
				t.Fatal(err)
			}
			defer closeDatabase(reopened.Handle)
			rows, err := runSQL(reopened.Handle, "SELECT id FROM t", "", modeQuery)
			if err != nil || len(rows.Rows) != 1 || rows.Rows[0][0] != int64(42) {
				t.Fatalf("reopened: %#v, %v", rows, err)
			}
		})
	}
	t.Run("encrypted json", func(t *testing.T) {
		key := base64.StdEncoding.EncodeToString([]byte(strings.Repeat("k", 32)))
		options := `{"mode":"json","path":` + jsonString(filepath.Join(dir, "secret")) + `,"encryptionKey":"` + key + `"}`
		opened, err := openWithOptions(options)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := executeScript(opened.Handle, "CREATE TABLE s (v TEXT); INSERT INTO s VALUES ('hidden')"); err != nil {
			t.Fatal(err)
		}
		if _, err := closeDatabase(opened.Handle); err != nil {
			t.Fatal(err)
		}
		reopened, err := openWithOptions(options)
		if err != nil {
			t.Fatal(err)
		}
		defer closeDatabase(reopened.Handle)
		rows, err := runSQL(reopened.Handle, "SELECT v FROM s", "", modeQuery)
		if err != nil || len(rows.Rows) != 1 || rows.Rows[0][0] != "hidden" {
			t.Fatalf("encrypted reopen: %#v, %v", rows, err)
		}
	})
	t.Run("read only", func(t *testing.T) {
		options := `{"mode":"json","path":` + jsonString(filepath.Join(dir, "json")) + `,"readOnly":true}`
		opened, err := openWithOptions(options)
		if err != nil {
			t.Fatal(err)
		}
		defer closeDatabase(opened.Handle)
		if _, err := runSQL(opened.Handle, "INSERT INTO t VALUES (1)", "", modeExec); err == nil {
			t.Fatal("read-only database accepted a write")
		}
	})
	t.Run("snapshot and memory", func(t *testing.T) {
		memory, err := openWithOptions("")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := executeScript(memory.Handle, "CREATE TABLE m (id INT); INSERT INTO m VALUES (7)"); err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(dir, "m.snapshot")
		if _, err := saveDatabase(memory.Handle, path); err != nil {
			t.Fatal(err)
		}
		closeDatabase(memory.Handle)
		snapshot, err := openWithOptions(`{"path":` + jsonString(path) + `,"readOnly":true}`)
		if err != nil {
			t.Fatal(err)
		}
		defer closeDatabase(snapshot.Handle)
		if _, err := runSQL(snapshot.Handle, "INSERT INTO m VALUES (8)", "", modeExec); err == nil {
			t.Fatal("read-only snapshot accepted a write")
		}
	})
	for _, bad := range []string{`{"mode":"nope","path":"x"}`, `{"mode":"disk"}`, `{"mode":"memory","path":"x"}`, `{"mode":"snapshot"}`, `{"unknown":1}`, `[]`, `{"mode":"json","path":"x","walSync":"sometimes"}`, `{"mode":"json","path":"x","encryptionKey":"!"}`, `{} {}`} {
		if opened, err := openWithOptions(bad); err == nil {
			closeDatabase(opened.Handle)
			t.Errorf("accepted options %s", bad)
		}
	}
}

func TestResponseEncodingMatchesJSON(t *testing.T) {
	when := time.Date(2026, 9, 27, 1, 2, 3, 4, time.UTC)
	value := response{
		Columns: []string{"a\"b", "ü"},
		Rows: [][]any{
			{int64(math.MinInt64), 0.1, 1e21, true, false, nil, "line\n\t\"q\" \\ \x01   🦊", string([]byte{0xff, 'x'})},
			{[]byte{0, 1, 2}, when, 7, map[string]int{"k": 1}, -0.0, 5e-324, "", []byte{}},
		},
		RowsAffected: 3,
	}
	encoded := encodeResponse(func() (response, error) { return value, nil })
	if !json.Valid(encoded) || !utf8.Valid(encoded) {
		t.Fatalf("invalid JSON: %s", encoded)
	}
	var decoded struct {
		Columns      []string `json:"columns"`
		Rows         [][]any  `json:"rows"`
		RowsAffected int64    `json:"rowsAffected"`
	}
	dec := json.NewDecoder(strings.NewReader(string(encoded)))
	dec.UseNumber()
	if err := dec.Decode(&decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.Columns[0] != `a"b` || decoded.RowsAffected != 3 {
		t.Fatalf("decoded: %#v", decoded)
	}
	first := decoded.Rows[0]
	if first[0] != json.Number("-9223372036854775808") || first[6] != "line\n\t\"q\" \\ \x01   🦊" || first[7] != "�x" {
		t.Fatalf("first row: %#v", first)
	}
	for i, want := range []float64{0.1, 1e21} {
		number, _ := first[i+1].(map[string]any)["real"].(json.Number).Float64()
		if number != want {
			t.Fatalf("real %d: %v", i, number)
		}
	}
	second := decoded.Rows[1]
	if second[0].(map[string]any)["blob"] != "AAEC" || second[1] != when.Format(time.RFC3339Nano) || second[7].(map[string]any)["blob"] != "" {
		t.Fatalf("second row: %#v", second)
	}
	if smallest, _ := second[5].(map[string]any)["real"].(json.Number).Float64(); smallest != 5e-324 {
		t.Fatalf("denormal: %v", smallest)
	}
}

func jsonString(s string) string {
	encoded, _ := json.Marshal(s)
	return string(encoded)
}

func BenchmarkQueryEncoding(b *testing.B) {
	id, err := openDatabase("")
	if err != nil {
		b.Fatal(err)
	}
	defer closeDatabase(id.Handle)
	if _, err := executeScript(id.Handle, "CREATE TABLE bench (id INT, name TEXT, score FLOAT, active BOOL)"); err != nil {
		b.Fatal(err)
	}
	var fixed strings.Builder
	fixed.WriteByte('[')
	for i := range 1000 {
		if i > 0 {
			fixed.WriteByte(',')
		}
		fixed.WriteString(`[` + itoa(i) + `,"name ` + strings.Repeat("x", i%16) + `",{"real":1.5},true]`)
	}
	fixed.WriteByte(']')
	if _, err := executeBatch(id.Handle, "INSERT INTO bench VALUES (?, ?, ?, ?)", fixed.String()); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for b.Loop() {
		encoded := encodeResponse(func() (response, error) {
			return runSQL(id.Handle, "SELECT id, name, score, active FROM bench", "", modeQuery)
		})
		if len(encoded) < 1000 {
			b.Fatal(string(encoded))
		}
	}
}

func itoa(i int) string { return string(appendInt(nil, int64(i))) }

func TestResponsesReportTransactionState(t *testing.T) {
	id := testDatabase(t)
	decode := func(fn func() (response, error)) map[string]any {
		var decoded map[string]any
		if err := json.Unmarshal(encodeResponse(fn), &decoded); err != nil {
			t.Fatal(err)
		}
		return decoded
	}
	if got := decode(func() (response, error) { return executeScript(id, "CREATE TABLE t (id INT); BEGIN") }); got["inTransaction"] != true {
		t.Fatalf("after BEGIN: %v", got)
	}
	failed := decode(func() (response, error) { return runSQL(id, "INSERT INTO missing VALUES (1)", "", modeAuto) })
	if failed["error"] == nil || failed["inTransaction"] != true || len(failed) != 2 {
		t.Fatalf("failed statement inside a transaction: %v", failed)
	}
	if got := decode(func() (response, error) { return runSQL(id, "INSERT INTO t VALUES (1)", "", modeAuto) }); got["inTransaction"] != true {
		t.Fatalf("inside transaction: %v", got)
	}
	if got := decode(func() (response, error) { return runSQL(id, "ROLLBACK", "", modeAuto) }); got["inTransaction"] != nil {
		t.Fatalf("after ROLLBACK: %v", got)
	}
	if got := decode(func() (response, error) { return runSQL(id, "SELECT COUNT(*) FROM t", "", modeAuto) }); got["rows"].([]any)[0].([]any)[0] != float64(0) {
		t.Fatalf("rolled back row visible: %v", got)
	}
}
