//go:build !wasm

package main

import (
	"testing"

	tinysql "github.com/SimonWaldherr/tinySQL"
)

func TestBuildResultPageFiltersSortsAndSlices(t *testing.T) {
	result := &tinysql.ResultSet{
		Cols: []string{"id", "title"},
		Rows: []tinysql.Row{
			{"id": 10, "title": "Vector basics"},
			{"id": 2, "title": "Database vectors"},
			{"id": 1, "title": "Unrelated"},
		},
	}

	page := buildResultPage(result, 0, 1, "vector", "id", "asc")
	if page.TotalRows != 3 || page.FilteredRows != 2 {
		t.Fatalf("counts = total %d, filtered %d", page.TotalRows, page.FilteredRows)
	}
	if len(page.Rows) != 1 || page.Rows[0]["id"] != 2 {
		t.Fatalf("first page = %#v, want numeric id 2", page.Rows)
	}

	page = buildResultPage(result, 1, 50, "vector", "id", "desc")
	if len(page.Rows) != 1 || page.Rows[0]["id"] != 2 {
		t.Fatalf("second descending page = %#v, want numeric id 2", page.Rows)
	}
}

func TestBuildResultPageClampsBoundsAndKeepsNullLast(t *testing.T) {
	result := &tinysql.ResultSet{
		Cols: []string{"value"},
		Rows: []tinysql.Row{
			{"value": nil},
			{"value": "10"},
			{"value": 2},
		},
	}

	page := buildResultPage(result, -10, maxResultPageSize+1, "", "value", "asc")
	if page.Offset != 0 || page.Limit != maxResultPageSize {
		t.Fatalf("bounds = offset %d, limit %d", page.Offset, page.Limit)
	}
	if len(page.Rows) != 3 || page.Rows[0]["value"] != 2 || page.Rows[2]["value"] != nil {
		t.Fatalf("sorted rows = %#v", page.Rows)
	}
}

func TestResultPagerReusesViewAndInvalidatesOnSortChange(t *testing.T) {
	result := &tinysql.ResultSet{
		Cols: []string{"id", "title"},
		Rows: []tinysql.Row{{"id": 3, "title": "vector"}, {"id": 1, "title": "vector"}, {"id": 2, "title": "other"}},
	}
	var pager resultPager
	first := pager.page(result, 0, 1, "vector", "id", "asc")
	second := pager.page(result, 1, 1, "vector", "id", "asc")
	if first.Rows[0]["id"] != 1 || second.Rows[0]["id"] != 3 {
		t.Fatalf("cached pages = %#v / %#v", first.Rows, second.Rows)
	}
	descending := pager.page(result, 0, 1, "vector", "id", "desc")
	if descending.Rows[0]["id"] != 3 {
		t.Fatalf("sort change did not invalidate cached index: %#v", descending.Rows)
	}
}
