package main

import (
	"fmt"
	"reflect"
	"testing"

	tinysql "github.com/SimonWaldherr/tinySQL"
)

func TestResultPagerViewTransitions(t *testing.T) {
	result := &tinysql.ResultSet{Cols: []string{"id"}, Rows: []tinysql.Row{
		{"id": 3}, {"id": 1}, {"id": 2},
	}}
	var pager resultPager
	for _, tc := range []struct {
		filter, column string
		want           []int
	}{
		{"", "", []int{3, 1, 2}},
		{"", "id", []int{1, 2, 3}},
		{"", "", []int{3, 1, 2}},
		{"missing", "", []int{}},
		{"", "unknown", []int{3, 1, 2}},
		{"2", "id", []int{2}},
		{"  ", "", []int{3, 1, 2}},
	} {
		page := pager.page(result, 0, 10, tc.filter, tc.column, "asc")
		got := make([]int, len(page.Rows))
		for i, row := range page.Rows {
			got[i] = row["id"].(int)
		}
		if !reflect.DeepEqual(got, tc.want) || page.FilteredRows != len(tc.want) || page.TotalRows != 3 {
			t.Fatalf("filter=%q column=%q: page=%+v, want IDs %v", tc.filter, tc.column, page, tc.want)
		}
	}
	pager.reset()
	page := pager.page(result, 1, 1, "", "", "asc")
	if len(page.Rows) != 1 || page.Rows[0]["id"] != 1 || page.Offset != 1 {
		t.Fatalf("page after reset: %+v", page)
	}
	other := &tinysql.ResultSet{Cols: []string{"id"}, Rows: []tinysql.Row{{"id": 9}}}
	page = pager.page(other, 0, 10, "", "", "asc")
	if len(page.Rows) != 1 || page.Rows[0]["id"] != 9 || page.TotalRows != 1 {
		t.Fatalf("replacement result page: %+v", page)
	}
}

func TestUnfilteredResultPageBoundsAndOwnership(t *testing.T) {
	result := &tinysql.ResultSet{Rows: []tinysql.Row{{"id": 0}, {"id": 1}, {"id": 2}}}
	for _, tc := range []struct{ offset, limit, wantOffset, wantRows int }{
		{-1, 0, 0, 3}, {1, 1, 1, 1}, {2, 100, 2, 1}, {3, 1, 3, 0}, {99, 1, 3, 0},
	} {
		page := buildResultPage(result, tc.offset, tc.limit, "", "", "asc")
		if page.Offset != tc.wantOffset || len(page.Rows) != tc.wantRows || page.FilteredRows != 3 {
			t.Fatalf("offset=%d limit=%d: %+v", tc.offset, tc.limit, page)
		}
		if len(page.Rows) > 0 {
			page.Rows[0] = tinysql.Row{"id": -1}
			if result.Rows[tc.wantOffset]["id"] != tc.wantOffset {
				t.Fatal("page row slice aliases source result")
			}
		}
	}
	for _, result := range []*tinysql.ResultSet{nil, {}, {Rows: []tinysql.Row{}}} {
		page := buildResultPage(result, 10, 1, "", "", "asc")
		if page.Rows == nil || len(page.Rows) != 0 || page.Offset != 0 || page.TotalRows != 0 || page.FilteredRows != 0 {
			t.Fatalf("empty result page: %+v", page)
		}
	}
}

func BenchmarkResultFirstPage(b *testing.B) {
	for _, size := range []int{1_000, 10_000, 100_000, 1_000_000} {
		result := &tinysql.ResultSet{Cols: []string{"id"}, Rows: make([]tinysql.Row, size)}
		row := tinysql.Row{"id": 1}
		for i := range result.Rows {
			result.Rows[i] = row
		}
		b.Run(fmt.Sprint(size), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				var pager resultPager
				page := pager.page(result, 0, defaultResultPageSize, "", "", "asc")
				if len(page.Rows) != defaultResultPageSize || page.TotalRows != size || page.FilteredRows != size {
					b.Fatalf("unexpected page: %+v", page)
				}
			}
		})
	}
}
