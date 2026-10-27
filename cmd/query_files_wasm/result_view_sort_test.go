package main

import (
	"fmt"
	"reflect"
	"strings"
	"testing"

	tinysql "github.com/SimonWaldherr/tinySQL"
)

func TestFilteredSortPreservesStableOrderAndPages(t *testing.T) {
	result := &tinysql.ResultSet{
		Cols: []string{"ID", "title", "value"},
		Rows: []tinysql.Row{
			{"id": 0, "title": "other", "value": -100},
			{"id": 1, "title": "match", "value": "2"},
			{"id": 2, "title": "match", "value": nil},
			{"id": 3, "title": "match", "value": 1},
			{"id": 4, "title": "match", "value": 2},
			{"id": 5, "title": "other", "value": 200},
			{"id": 6, "title": "other", "value": 7},
			{"id": 7, "title": "other", "value": 8},
			{"id": 8, "title": "other", "value": 9},
			{"id": 9, "title": "other", "value": 10},
			{"id": 10, "title": "other", "value": 11},
		},
	}
	for _, tc := range []struct {
		direction string
		want      []int
	}{{"asc", []int{3, 1, 4, 2}}, {"desc", []int{2, 1, 4, 3}}} {
		indices := buildResultRowIndexes(result, "match", "value", tc.direction)
		if !reflect.DeepEqual(indices, tc.want) {
			t.Errorf("%s indices = %v, want %v", tc.direction, indices, tc.want)
		}
		page := sliceResultPage(result, indices, 1, 2)
		if page.FilteredRows != 4 || len(page.Rows) != 2 || page.Rows[0]["id"] != 1 || page.Rows[1]["id"] != 4 {
			t.Errorf("%s page = %#v", tc.direction, page)
		}
	}
	if got := buildResultRowIndexes(result, "missing", "value", "asc"); len(got) != 0 {
		t.Fatalf("no-match indices = %v", got)
	}
}

func BenchmarkBuildResultRowIndexesFilteredSort(b *testing.B) {
	result := &tinysql.ResultSet{Cols: []string{"id", "title"}, Rows: make([]tinysql.Row, 10000)}
	for i := range result.Rows {
		title := "ordinary"
		if i%100 == 0 {
			title = "needle"
		}
		result.Rows[i] = tinysql.Row{"id": fmt.Sprintf("%06d", 10000-i), "title": title}
	}
	for _, tc := range []struct{ name, filter string }{
		{"rare", "needle"}, {"broad", "ordinary"}, {"unfiltered", ""},
	} {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				buildResultRowIndexes(result, tc.filter, "id", "asc")
			}
		})
	}
}

func TestResultSortUsesNormalizedColumnKeys(t *testing.T) {
	for _, filter := range []string{"", "match"} {
		result := &tinysql.ResultSet{Cols: []string{"ÜBERSCHRIFT", "status"}, Rows: []tinysql.Row{
			{"überschrift": "b", "status": "match"},
			{"überschrift": "a", "status": "match"},
			{"überschrift": "c", "status": "other"},
			{"überschrift": "d", "status": "other"},
			{"überschrift": "e", "status": "other"},
		}}
		want := []int{1, 0, 2, 3, 4}
		if filter != "" {
			want = []int{1, 0}
		}
		if got := buildResultRowIndexes(result, filter, "ÜBERSCHRIFT", "asc"); !reflect.DeepEqual(got, want) {
			t.Fatalf("filter %q: indexes = %v, want %v", filter, got, want)
		}
	}
}

func BenchmarkResultSortColumnKeys(b *testing.B) {
	for _, column := range []string{"value", "VALUE", "ÜBERSCHRIFT"} {
		rows := make([]tinysql.Row, 10000)
		key := strings.ToLower(column)
		for i := range rows {
			rows[i] = tinysql.Row{key: fmt.Sprintf("%05d", 10000-i)}
		}
		result := &tinysql.ResultSet{Cols: []string{column}, Rows: rows}
		b.Run(column, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				buildResultRowIndexes(result, "", column, "asc")
			}
		})
	}
}
