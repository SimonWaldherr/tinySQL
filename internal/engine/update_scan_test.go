package engine

import (
	"reflect"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func BenchmarkUpdateReturningSelective(b *testing.B) {
	db := setupPerfTable(b, 20_000)
	b.Cleanup(func() { _ = db.Close() })
	runBench(b, db, "UPDATE t SET val = val + 1 WHERE id < 200 RETURNING id, val")
}

func TestSelectiveUpdateReturningAndRollback(t *testing.T) {
	db := storage.NewDB()
	t.Cleanup(func() { _ = db.Close() })
	table := storage.NewTable("events", []storage.Column{
		{Name: "id", Type: storage.IntType},
		{Name: "count", Type: storage.IntType},
	}, false)
	for i := range 128 {
		table.Rows = append(table.Rows, []any{i, 0})
	}
	table.Rows = append(table.Rows, []any{nil, 0})
	if err := db.Put("default", table); err != nil {
		t.Fatal(err)
	}
	returning := executeIndexSQL(t, db, "UPDATE events SET count = count + 1 WHERE 126 <= events.id RETURNING id, count")
	wantRows := []Row{{"id": 126, "count": 1}, {"id": 127, "count": 1}}
	if !reflect.DeepEqual(returning.Rows, wantRows) {
		t.Fatalf("RETURNING = %#v, want %#v", returning.Rows, wantRows)
	}
	// The compiled comparison must preserve the interpreter's type errors.
	incompatible := mustParse("UPDATE events SET count = 2 WHERE events.id = 'text' RETURNING id")
	if _, err := Execute(t.Context(), db, "default", incompatible); err == nil {
		t.Fatal("expected an incomparable-type error")
	}
	before := append([][]any(nil), table.Rows...)
	badUpdate := mustParse("UPDATE events SET count = 100 / (id - 90) WHERE id < 105 RETURNING id")
	if _, err := Execute(t.Context(), db, "default", badUpdate); err == nil {
		t.Fatal("expected division by zero after matching earlier rows")
	}
	table, err := db.Get("default", "events")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, table.Rows) {
		t.Fatal("partial UPDATE survived rollback")
	}
	retry := executeIndexSQL(t, db, "UPDATE events SET count = count + 1 WHERE id < 105 RETURNING id")
	if got := len(retry.Rows); got != 105 {
		t.Fatalf("retried UPDATE returned %d rows, want 105", got)
	}
	last := executeIndexSQL(t, db, "SELECT count FROM events WHERE id = 90")
	if got := last.Rows[0]["count"]; got != 1 {
		t.Fatalf("row 90 count = %v, want 1", got)
	}
}
