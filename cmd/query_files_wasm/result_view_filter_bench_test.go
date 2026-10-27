package main

import (
	"fmt"
	"reflect"
	"testing"

	tinysql "github.com/SimonWaldherr/tinySQL"
)

func TestResultFilterCaseInsensitiveColumnKeys(t *testing.T) {
	result := &tinysql.ResultSet{
		Cols: []string{"ID", "ÜBERSCHRIFT", "MISSING"},
		Rows: []tinysql.Row{
			{"id": 1, "überschrift": "München"},
			{"id": 2, "überschrift": "Berlin"},
			{"id": 3, "überschrift": nil},
		},
	}
	got := buildResultPage(result, 0, 10, "MÜNCHEN", "", "asc")
	if got.FilteredRows != 1 || !reflect.DeepEqual(got.Rows, []tinysql.Row{result.Rows[0]}) {
		t.Fatalf("filtered page = %#v", got)
	}
}

// Upper-case column labels exercise the case-insensitive lookup performed for
// every cell during filtering, independently of sorting and page rendering.
func BenchmarkResultFilterUppercaseColumns(b *testing.B) {
	result := &tinysql.ResultSet{Cols: []string{"FIRST_NAME", "LAST_NAME", "CATEGORY", "CITY", "COUNTRY", "REFERENCE"}, Rows: make([]tinysql.Row, 10000)}
	for i := range result.Rows {
		result.Rows[i] = tinysql.Row{
			"first_name": "Anna", "last_name": "Smith", "category": "local",
			"city": "Berlin", "country": "Germany", "reference": fmt.Sprintf("item-%d", i),
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buildResultRowIndexes(result, "not-present", "", "asc")
	}
}
