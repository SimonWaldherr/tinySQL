package engine

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestDateComponentCalendarValues(t *testing.T) {
	plan := &simpleSelectPlan{colIndex: map[string]int{"d": 0}}
	value := "2020-12-31T23:59:58.123+05:45"
	for _, tc := range []struct {
		name string
		want int
	}{
		{"YEAR", 2020}, {"MONTH", 12}, {"DAY", 31}, {"HOUR", 23}, {"MINUTE", 59},
		{"SECOND", 58}, {"DAYOFWEEK", 5}, {"DAYOFYEAR", 366}, {"WEEKOFYEAR", 53}, {"QUARTER", 4},
	} {
		expr := mustParse("SELECT " + tc.name + "(d)").(*Select).Projs[0].Expr
		got, err := evalRawExpr(plan, []any{value}, expr)
		if err != nil || got != tc.want {
			t.Fatalf("%s: value=%v error=%v, want %v", tc.name, got, err, tc.want)
		}
		for _, arguments := range []string{"", "NULL", "d", "missing", "1/0", "NULL, 1/0", "'invalid'"} {
			expr := mustParse("SELECT " + tc.name + "(" + arguments + ")").(*Select).Projs[0].Expr
			for _, value := range []any{nil, "2024-02-29", "invalid", time.Date(2024, 3, 31, 1, 45, 12, 0, time.FixedZone("test", 20730))} {
				got, err := evalRawExpr(plan, []any{value}, expr)
				want, wantErr := evalExpr(ExecEnv{}, expr, Row{"d": value})
				if !reflect.DeepEqual(got, want) || fmt.Sprint(err) != fmt.Sprint(wantErr) {
					t.Fatalf("%s(%s), d=%v: got %v/%v, want %v/%v", tc.name, arguments, value, got, err, want, wantErr)
				}
			}
		}
	}
}

func TestDateComponentQueryPaths(t *testing.T) {
	db := storage.NewDB()
	t.Cleanup(func() { _ = db.Close() })
	execSQL(t, db, "CREATE TABLE date_rows (id INT, d TEXT)")
	execSQL(t, db, "INSERT INTO date_rows VALUES (1, '2020-12-31'), (2, '2021-01-01'), (3, NULL), (4, '2024-02-29T23:30:00-05:00')")
	query := "SELECT id, YEAR(d) AS yr, MONTH(d) AS mo FROM date_rows WHERE d IS NOT NULL ORDER BY id"
	want := []Row{{"id": 1, "yr": 2020, "mo": 12}, {"id": 2, "yr": 2021, "mo": 1}, {"id": 4, "yr": 2024, "mo": 2}}
	result := execSQL(t, db, query)
	if !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("date projection: got %v, want %v", result.Rows, want)
	}
	stream, err := ExecuteStream(t.Context(), db, "default", mustParse(query))
	if err != nil {
		t.Fatal(err)
	}
	var rows []Row
	for stream.Next() {
		rows = append(rows, stream.Row())
	}
	streamErr := stream.Err()
	_ = stream.Close()
	if streamErr != nil || !reflect.DeepEqual(rows, want) {
		t.Fatalf("date stream: rows=%v error=%v", rows, streamErr)
	}
	columns, err := ExecuteColumnar(t.Context(), db, "default", mustParse(query))
	if err != nil || !reflect.DeepEqual(*columns, columnarFromRows(result)) {
		t.Fatalf("date columnar result=%v error=%v", columns, err)
	}
	for _, query := range []string{
		"SELECT COUNT(*) AS n FROM date_rows WHERE YEAR(d) = 2020",
		"SELECT COUNT(*) AS n FROM date_rows WHERE MONTH(d) = 2",
	} {
		if result := execSQL(t, db, query); result.Rows[0]["n"] != 1 {
			t.Fatalf("%s: %v", query, result.Rows)
		}
	}
	if result := execSQL(t, db, "SELECT SUM(MONTH(d)) AS n FROM date_rows"); result.Rows[0]["n"] != 15 {
		t.Fatalf("date aggregate: %v", result.Rows)
	}
}

func TestFixedDateMonthBoundaries(t *testing.T) {
	for _, year := range []int{0, 1, 4, 100, 400, 1900, 2000, 2024, 2100, 2400, 9999} {
		for month := 1; month <= 12; month++ {
			for day := 28; day <= 32; day++ {
				input := fmt.Sprintf("%04d-%02d-%02d", year, month, day)
				got, accepted := parseTimeFixedDigits(input)
				want, err := time.Parse("2006-01-02", input)
				if accepted != (err == nil) || accepted && !got.Equal(want) {
					t.Fatalf("%s: got %v accepted=%v, want %v error=%v", input, got, accepted, want, err)
				}
			}
		}
	}
}
