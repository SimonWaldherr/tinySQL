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

func TestInitialPatterns(t *testing.T) {
	for _, tc := range []struct {
		name  string
		count int
	}{{"random", -1}, {"block", 4}, {"blinker", 3}, {"glider", 5}} {
		t.Run(tc.name, func(t *testing.T) {
			first, err := initialCells(5, 5, tc.name, 42)
			if err != nil {
				t.Fatal(err)
			}
			second, err := initialCells(5, 5, tc.name, 42)
			if err != nil || !reflect.DeepEqual(first, second) {
				t.Fatal("pattern is not reproducible")
			}
			count := 0
			for cell, alive := range first {
				if cell[0] < 0 || cell[0] >= 5 || cell[1] < 0 || cell[1] >= 5 {
					t.Fatalf("out of bounds: %v", cell)
				}
				if alive {
					count++
				}
			}
			if tc.count >= 0 && count != tc.count {
				t.Fatalf("got %d live cells", count)
			}
		})
	}
	if _, err := initialCells(5, 5, "unknown", 1); err == nil {
		t.Fatal("unknown pattern accepted")
	}
}

func TestGliderMovesAfterFourGenerations(t *testing.T) {
	if err := registerStep(); err != nil {
		t.Fatal(err)
	}
	initial, err := initialCells(8, 8, "glider", 1)
	if err != nil {
		t.Fatal(err)
	}
	db := tinysql.NewDB()
	if err := createBoard(context.Background(), db, 8, 8, initial); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 4; i++ {
		if _, err := query(context.Background(), db, `CALL life_step()`); err != nil {
			t.Fatal(err)
		}
	}
	want := map[[2]int]bool{}
	for cell := range initial {
		want[[2]int{cell[0] + 1, cell[1] + 1}] = true
	}
	if got := boardLive(t, db); !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v want %v", got, want)
	}
}
