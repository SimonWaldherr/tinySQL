package main

import (
	"context"
	"fmt"
	tinysql "github.com/SimonWaldherr/tinySQL"
	"reflect"
	"testing"
)

func boardLive(t *testing.T, db *tinysql.DB) map[[2]int]bool {
	t.Helper()
	rs, err := query(context.Background(), db, `SELECT x, y FROM cells WHERE alive = 1`)
	if err != nil {
		t.Fatal(err)
	}
	live := map[[2]int]bool{}
	for _, r := range rs.Rows {
		var x, y int
		fmt.Sscan(fmt.Sprint(r["x"]), &x)
		fmt.Sscan(fmt.Sprint(r["y"]), &y)
		live[[2]int{x, y}] = true
	}
	return live
}
func TestSQLGameOfLife(t *testing.T) {
	if err := registerStep(); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name          string
		initial, next map[[2]int]bool
	}{
		{"blinker", map[[2]int]bool{{1, 2}: true, {2, 2}: true, {3, 2}: true}, map[[2]int]bool{{2, 1}: true, {2, 2}: true, {2, 3}: true}},
		{"block", map[[2]int]bool{{1, 1}: true, {1, 2}: true, {2, 1}: true, {2, 2}: true}, map[[2]int]bool{{1, 1}: true, {1, 2}: true, {2, 1}: true, {2, 2}: true}},
		{"wrap", map[[2]int]bool{{4, 2}: true, {0, 2}: true, {1, 2}: true}, map[[2]int]bool{{0, 1}: true, {0, 2}: true, {0, 3}: true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := tinysql.NewDB()
			if err := createBoard(context.Background(), db, 5, 5, tc.initial); err != nil {
				t.Fatal(err)
			}
			if _, err := query(context.Background(), db, `CALL life_step()`); err != nil {
				t.Fatal(err)
			}
			if got := boardLive(t, db); !reflect.DeepEqual(got, tc.next) {
				t.Fatalf("got %v want %v", got, tc.next)
			}
		})
	}
}
