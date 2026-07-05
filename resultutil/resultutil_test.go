package resultutil

import (
	"testing"

	tinysql "github.com/SimonWaldherr/tinySQL"
)

func TestResultSetStringMatrixRoundTrip(t *testing.T) {
	rs := &tinysql.ResultSet{
		Cols: []string{"id", "name"},
		Rows: []tinysql.Row{
			{"id": int64(1), "name": "Ada"},
			{"id": int64(2), "name": []byte("Grace")},
		},
	}
	cols, rows := ResultSetToStringMatrix(rs)
	if len(cols) != 2 || cols[0] != "id" || rows[1][1] != "Grace" {
		t.Fatalf("unexpected matrix cols=%v rows=%v", cols, rows)
	}
	back := StringMatrixToResultSet(cols, rows)
	if got, ok := tinysql.GetVal(back.Rows[0], "ID"); !ok || got != "1" {
		t.Fatalf("expected case-insensitive id value, got %#v ok=%v", got, ok)
	}
}

func TestSummarizeMatrixProfilesLargeResult(t *testing.T) {
	rows := [][]string{
		{"open", "10"},
		{"open", "15"},
		{"closed", "20"},
	}
	summary := SummarizeMatrix([]string{"status", "amount"}, rows, SummaryOptions{
		MaxRows:      2,
		MaxExamples:  2,
		MaxTopValues: 2,
	})
	if !summary.Summarized || summary.TotalRows != 3 || len(summary.Rows) != 2 {
		t.Fatalf("unexpected summary: %#v", summary)
	}
	if len(summary.Profile) != 2 {
		t.Fatalf("expected profiles, got %#v", summary.Profile)
	}
	if summary.Profile[0].TopValues[0].Value != "open" || summary.Profile[0].TopValues[0].Count != 2 {
		t.Fatalf("unexpected top values: %#v", summary.Profile[0].TopValues)
	}
	if !summary.Profile[1].Numeric || summary.Profile[1].Sum != "45" || summary.Profile[1].Avg != "15" {
		t.Fatalf("unexpected numeric profile: %#v", summary.Profile[1])
	}
}
