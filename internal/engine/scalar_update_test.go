package engine

import (
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestBoundScalarUpdateRollbackAndOriginalValues(t *testing.T) {
	db := storage.NewDB()
	t.Cleanup(func() { _ = db.Close() })
	execSQL(t, db, "CREATE TABLE scalar_updates (id INT PRIMARY KEY,label TEXT,v INT,flag BOOL)")
	values := make([]string, 130)
	for i := range values {
		values[i] = fmt.Sprintf("(%d,'xxAlpha xx',%d,FALSE)", i, i%3)
	}
	execSQL(t, db, "INSERT INTO scalar_updates VALUES "+strings.Join(values, ","))
	execSQL(t, db, "CREATE INDEX idx_scalar_label ON scalar_updates(label)")
	before := execSQL(t, db, "SELECT * FROM scalar_updates ORDER BY id")
	if _, err := Execute(t.Context(), db, "default", mustParse(
		"UPDATE scalar_updates SET label=TRIM(label,IF(id=129,1/0,' x')),v=COALESCE(NULLIF(v,0),7)")); err == nil {
		t.Fatal("expected a late cutset error")
	}
	if got := execSQL(t, db, "SELECT * FROM scalar_updates ORDER BY id"); !reflect.DeepEqual(got, before) {
		t.Fatal("failed scalar update changed stored rows")
	}
	if got := execSQL(t, db, "SELECT COUNT(*) AS n FROM scalar_updates WHERE label='xxAlpha xx'"); got.Rows[0]["n"] != 130 {
		t.Fatal("failed scalar update changed the secondary index", got)
	}
	execSQL(t, db, `UPDATE scalar_updates SET label=LOWER(TRIM(label,' x')),
		v=COALESCE(NULLIF(v,0),LENGTH(TRIM(label,' x'))),flag=COALESCE(v,0) BETWEEN 1 AND 2`)
	got := execSQL(t, db, "SELECT id,label,v,flag FROM scalar_updates ORDER BY id")
	for i, row := range got.Rows {
		v := i % 3
		if v == 0 {
			v = 5
		}
		want := Row{"id": i, "label": "alpha", "v": v, "flag": i%3 != 0}
		if !reflect.DeepEqual(row, want) {
			t.Fatalf("row %d: %v, want %v", i, row, want)
		}
	}
	if got := execSQL(t, db, "SELECT COUNT(*) AS n FROM scalar_updates WHERE label='alpha'"); got.Rows[0]["n"] != 130 {
		t.Fatal("trimmed update lost index entries", got)
	}
	execSQL(t, db, "UPDATE scalar_updates SET flag=v IS NULL")
	execSQL(t, db, "UPDATE scalar_updates SET v=NULLIF(v,v),flag=ISNULL(v)")
	if got := execSQL(t, db, "SELECT COUNT(*) AS n FROM scalar_updates WHERE v IS NULL AND flag=FALSE"); got.Rows[0]["n"] != 130 {
		t.Fatal("assignments did not read original NULL values", got)
	}
}
