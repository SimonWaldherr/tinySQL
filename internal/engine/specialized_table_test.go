package engine

import (
	"bytes"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func checkSpecializedTables(t *testing.T, db *storage.DB) {
	t.Helper()
	kv := executeIndexSQL(t, db, `SELECT value FROM kv WHERE key = 'a'`)
	if len(kv.Rows) != 1 || string(kv.Rows[0]["value"].([]byte)) != "bye" {
		t.Fatal(kv.Rows)
	}
	binaryValue := executeIndexSQL(t, db, `SELECT value FROM kv WHERE key = 'binary'`)
	if len(binaryValue.Rows) != 1 || !bytes.Equal(binaryValue.Rows[0]["value"].([]byte), []byte{0, 255, 128}) {
		t.Fatal(binaryValue.Rows)
	}
	doc := executeIndexSQL(t, db, `SELECT JSON_EXTRACT(document, 'name') AS name FROM docs WHERE id = 'one'`)
	if len(doc.Rows) != 1 || doc.Rows[0]["name"] != "Ada" {
		t.Fatal(doc.Rows)
	}
	executeIndexSQL(t, db, `UPDATE docs SET document = document WHERE id = 'string'`)
	str := executeIndexSQL(t, db, `SELECT document FROM docs WHERE id = 'string'`)
	if len(str.Rows) != 1 || len(str.Rows[0]["document"].([]any)) != 1 || str.Rows[0]["document"].([]any)[0] != "123" {
		t.Fatalf("JSON string changed type: %v", str.Rows)
	}
	rs := executeIndexSQL(t, db, `SELECT value FROM samples WHERE series = 'cpu' AND time >= 10 AND time < 20 ORDER BY time`)
	if len(rs.Rows) != 2 {
		t.Fatal(rs.Rows)
	}
	table, err := db.Get("default", "samples")
	if err != nil {
		t.Fatal(err)
	}
	if table.FindSecondaryIndex([]string{"series", "time"}) == nil {
		t.Fatal("time-series index missing")
	}
	for _, query := range []string{`INSERT INTO docs VALUES ('bad', 'not json')`, `INSERT INTO docs VALUES ('scalar', '"123"')`, `UPDATE docs SET document = 'broken' WHERE id = 'one'`, `INSERT INTO samples VALUES (NULL, 1, 1)`, `INSERT INTO docs VALUES ('null', NULL)`} {
		if _, err := Execute(t.Context(), db, "default", mustParse(query)); err == nil {
			t.Fatalf("accepted %s", query)
		}
	}
}

func TestSpecializedTablesStorageModes(t *testing.T) {
	for _, mode := range cpuBenchmarkModes {
		t.Run(mode.String(), func(t *testing.T) {
			db, cfg := cpuModeFixture(t, mode)
			for _, query := range []string{
				`CREATE VIRTUAL TABLE kv USING keyvalue`,
				`CREATE VIRTUAL TABLE docs USING document`,
				`CREATE VIRTUAL TABLE samples USING timeseries`,
				`INSERT INTO kv VALUES ('a', X'6869'), ('binary', X'00ff80')`,
				`UPDATE kv SET value = X'627965' WHERE key = 'a'`,
				`INSERT INTO docs VALUES ('one', '{"name":"Ada"}'), ('string', '["123"]')`,
				`INSERT INTO samples VALUES ('cpu', 9, 1.0), ('cpu', 10, 2.0), ('cpu', 10, 3.0), ('cpu', 20, 4.0), ('ram', 10, 5.0)`,
			} {
				executeIndexSQL(t, db, query)
			}
			checkSpecializedTables(t, db)
			for _, query := range []string{`INSERT INTO kv VALUES ('a', X'00')`, `INSERT INTO kv VALUES (NULL, X'00')`, `INSERT INTO docs VALUES ('null', NULL)`, `INSERT INTO samples VALUES (NULL, 1, 1)`} {
				if _, err := Execute(t.Context(), db, "default", mustParse(query)); err == nil {
					t.Fatalf("accepted %s", query)
				}
			}
			explain := executeIndexSQL(t, db, `EXPLAIN SELECT value FROM kv WHERE key = 'a'`)
			if !strings.Contains(fmt.Sprint(explain.Rows), "INDEX POINT SEEK") {
				t.Fatal(explain.Rows)
			}
			explain = executeIndexSQL(t, db, `EXPLAIN SELECT value FROM samples WHERE series = 'cpu' AND time >= 10 AND time < 20`)
			if !strings.Contains(fmt.Sprint(explain.Rows), "INDEX RANGE SCAN") {
				t.Fatal(explain.Rows)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			reopened, err := storage.OpenDB(cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			checkSpecializedTables(t, reopened)
		})
	}
}

func TestSpecializedTableDefinitions(t *testing.T) {
	db := storage.NewDB()
	defer db.Close()
	executeIndexSQL(t, db, `CREATE VIRTUAL TABLE renamed USING keyvalue(cache_key, payload)`)
	executeIndexSQL(t, db, `INSERT INTO renamed VALUES ('a', X'00')`)
	executeIndexSQL(t, db, `CREATE VIRTUAL TABLE IF NOT EXISTS renamed USING keyvalue`)
	rs := executeIndexSQL(t, db, `SELECT payload FROM renamed WHERE cache_key = 'a'`)
	if len(rs.Rows) != 1 {
		t.Fatal(rs.Rows)
	}
	for _, query := range []string{`CREATE VIRTUAL TABLE bad USING unknown`, `CREATE VIRTUAL TABLE bad USING keyvalue(k)`, `CREATE VIRTUAL TABLE bad USING document(ID, id)`} {
		if _, err := Execute(t.Context(), db, "default", mustParse(query)); err == nil {
			t.Fatalf("accepted %s", query)
		}
		if _, err := db.Get("default", "bad"); err == nil {
			t.Fatal("invalid profile published a table")
		}
	}
}

func BenchmarkKeyValueProfile(b *testing.B) {
	for _, indexed := range []bool{false, true} {
		b.Run(fmt.Sprint(indexed), func(b *testing.B) {
			db := storage.NewDB()
			defer db.Close()
			sql := "CREATE TABLE kv (key TEXT, value BLOB)"
			if indexed {
				sql = "CREATE VIRTUAL TABLE kv USING keyvalue"
			}
			if _, err := Execute(b.Context(), db, "default", mustParse(sql)); err != nil {
				b.Fatal(err)
			}
			table, _ := db.Get("default", "kv")
			for i := 0; i < 20000; i++ {
				table.Rows = append(table.Rows, []any{fmt.Sprint(i), []byte("value")})
			}
			table.Version++
			if err := table.RebuildSecondaryIndexes(); err != nil {
				b.Fatal(err)
			}
			stmt := mustParse("SELECT value FROM kv WHERE key = '10000'")
			// Warm the derived hash index before measuring repeated lookups.
			if _, err := Execute(b.Context(), db, "default", stmt); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			for b.Loop() {
				rs, err := Execute(b.Context(), db, "default", stmt)
				if err != nil || len(rs.Rows) != 1 {
					b.Fatalf("lookup: %v %v", rs, err)
				}
			}
		})
	}
}

func TestSpecializedTableWALRecovery(t *testing.T) {
	path := filepath.Join(t.TempDir(), "specialized.wal")
	schema := []string{`CREATE VIRTUAL TABLE kv USING keyvalue`, `CREATE VIRTUAL TABLE docs USING document`, `CREATE VIRTUAL TABLE samples USING timeseries`}
	live := storage.NewDB()
	for _, sql := range schema {
		executeIndexSQL(t, live, sql)
	}
	wal, err := storage.OpenAdvancedWAL(storage.AdvancedWALConfig{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	live.AttachAdvancedWAL(wal)
	for _, sql := range []string{
		`INSERT INTO kv VALUES ('a', X'6869'), ('binary', X'00ff80')`,
		`UPDATE kv SET value = X'627965' WHERE key = 'a'`,
		`INSERT INTO docs VALUES ('one', '{"name":"Ada"}'), ('string', '["123"]')`,
		`INSERT INTO samples VALUES ('cpu', 10, 2.0), ('cpu', 10, 3.0)`,
	} {
		executeIndexSQL(t, live, sql)
	}
	if err := wal.Close(); err != nil {
		t.Fatal(err)
	}
	defer live.Close()
	recovered := storage.NewDB()
	defer recovered.Close()
	for _, sql := range schema {
		executeIndexSQL(t, recovered, sql)
	}
	replay, err := storage.OpenAdvancedWAL(storage.AdvancedWALConfig{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	defer replay.Close()
	if _, err := replay.Recover(recovered); err != nil {
		t.Fatal(err)
	}
	checkSpecializedTables(t, recovered)
}
