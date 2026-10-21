package driver

import (
	"database/sql"
	"testing"
)

func TestSQLPreparedAggregateBinary(t *testing.T) {
	db := orderByFixture(t)
	stmt, err := db.Prepare("SELECT SUM(id * ?) AS total, AVG(? + score) AS mean FROM t")
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()
	for _, tc := range []struct {
		factor, offset any
		total, mean    float64
		valid          bool
	}{
		{2, 5, 12, 25, true},
		{0.5, 0.25, 3, 20.25, true},
		{-3, -10, -18, 10, true},
		{nil, nil, 0, 0, false},
		{7, 2, 42, 22, true},
	} {
		var total, mean sql.NullFloat64
		if err := stmt.QueryRow(tc.factor, tc.offset).Scan(&total, &mean); err != nil {
			t.Fatal(err)
		}
		if total.Valid != tc.valid || mean.Valid != tc.valid || (tc.valid && (total.Float64 != tc.total || mean.Float64 != tc.mean)) {
			t.Fatalf("factor %v, offset %v: got total %v, mean %v; want %v, %v (valid %v)", tc.factor, tc.offset, total, mean, tc.total, tc.mean, tc.valid)
		}
	}
}
