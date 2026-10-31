package importer

import (
	"fmt"
	"io"
	"reflect"
	"strings"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestJSONLineRecordsOwnDecodedStrings(t *testing.T) {
	var input strings.Builder
	want := make([]string, 40)
	for i := range want {
		want[i] = fmt.Sprintf("record-%d-", i) + strings.Repeat("é", 3000)
		fmt.Fprintf(&input, " \t{\"name\":%q,\"nested\":{\"value\":%q}}\t\n\n", want[i], want[i])
		if i == 5 {
			input.WriteString("not JSON\n")
		}
	}
	result := &ImportResult{}
	next, err := streamJSONRecords(strings.NewReader(input.String()), result, 0)
	if err != nil {
		t.Fatal(err)
	}
	var records []map[string]any
	for {
		record, err := next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		records = append(records, record)
	}
	if len(records) != len(want) || len(result.Errors) != 1 {
		t.Fatalf("records=%d, decode errors=%v", len(records), result.Errors)
	}
	// Several scanner buffer refills must not change any previously decoded
	// strings, including values in nested objects.
	for i, record := range records {
		nested, ok := record["nested"].(map[string]any)
		if !ok || record["name"] != want[i] || nested["value"] != want[i] {
			t.Fatalf("record %d changed after reading subsequent records", i)
		}
	}
}

func TestImportJSONNullsRemainNull(t *testing.T) {
	records := []string{`{"id":2,"name":"Ada"}`, `{"id":null,"name":null}`, `{}`, `{"id":3,"name":"Grace"}`}
	for _, input := range []string{strings.Join(records, "\n"), "[" + strings.Join(records, ",") + "]"} {
		for _, strict := range []bool{false, true} {
			db := storage.NewDB()
			t.Cleanup(func() { _ = db.Close() })
			opts := &ImportOptions{CreateTable: true, TypeInference: true, SampleRecords: 1, BatchSize: 1, StrictTypes: strict}
			result, err := ImportJSON(t.Context(), db, "default", "records", strings.NewReader(input), opts)
			if err != nil || result.RowsInserted != 4 || result.RowsSkipped != 0 || len(result.Errors) != 0 {
				t.Fatalf("strict=%v: result=%+v error=%v", strict, result, err)
			}
			table, err := db.Get("default", "records")
			if err != nil {
				t.Fatal(err)
			}
			for _, row := range table.Rows[1:3] {
				for _, value := range row {
					if value != nil {
						t.Fatalf("strict=%v: JSON null or missing value became %#v", strict, value)
					}
				}
			}
		}
	}
}

func TestFuzzyJSONNullsDoNotChangeColumnTypes(t *testing.T) {
	records := []string{`{"id":2}`, `{"id":null}`, `{}`, `{"id":3}`}
	for _, input := range []string{strings.Join(records, "\n"), "[" + strings.Join(records, ",") + "]"} {
		db := storage.NewDB()
		t.Cleanup(func() { _ = db.Close() })
		opts := &FuzzyImportOptions{ImportOptions: &ImportOptions{CreateTable: true, TypeInference: true}}
		result, err := FuzzyImportJSON(t.Context(), db, "default", "records", strings.NewReader(input), opts)
		if err != nil || result.RowsInserted != 4 || len(result.ColumnTypes) != 1 || result.ColumnTypes[0] != storage.IntType {
			t.Fatalf("nullable integer column: result=%+v error=%v", result, err)
		}
		table, err := db.Get("default", "records")
		if err != nil {
			t.Fatal(err)
		}
		if want := [][]any{{int(2)}, {nil}, {nil}, {int(3)}}; !reflect.DeepEqual(table.Rows, want) {
			t.Fatalf("rows=%#v, want %#v", table.Rows, want)
		}
	}
}

func TestFuzzyJSONImportPreservesValuesWithAndWithoutInference(t *testing.T) {
	lines := []string{
		`{"id":2,"name":"O'Reilly","active":true,"meta":{"enabled":true},"items":[1,"two",null]}`,
		`{"id":3,"name":"{color:blue}","active":false,"late":"later"}`,
	}
	for _, input := range []string{strings.Join(lines, "\n"), "[" + strings.Join(lines, ",") + "]"} {
		for _, inference := range []bool{false, true} {
			db := storage.NewDB()
			t.Cleanup(func() { _ = db.Close() })
			opts := &FuzzyImportOptions{ImportOptions: &ImportOptions{CreateTable: true, TypeInference: inference}}
			result, err := FuzzyImportJSON(t.Context(), db, "default", "records", strings.NewReader(input), opts)
			if err != nil || result.RowsInserted != 2 {
				t.Fatalf("inference=%v: result=%+v error=%v", inference, result, err)
			}
			table, err := db.Get("default", "records")
			if err != nil {
				t.Fatal(err)
			}
			wantID := any(float64(2))
			if inference {
				wantID = int(2)
			}
			for _, column := range []struct {
				name          string
				first, second any
			}{
				{"id", wantID, nil},
				{"name", "O'Reilly", "{color:blue}"},
				{"active", true, false},
				{"meta", `{"enabled":true}`, nil},
				{"items", `[1,"two",null]`, nil},
				{"late", nil, "later"},
			} {
				index, err := table.ColIndex(column.name)
				if err != nil {
					t.Fatal(err)
				}
				if !inference && table.Cols[index].Type != storage.TextType {
					t.Fatalf("column %s: inference disabled, type=%v", column.name, table.Cols[index].Type)
				}
				if table.Rows[0][index] != column.first || (column.name != "id" && table.Rows[1][index] != column.second) {
					t.Fatalf("column %s inference=%v: values=%v / %v", column.name, inference, table.Rows[0][index], table.Rows[1][index])
				}
			}
		}
	}
}

func TestFuzzyJSONDocumentClassification(t *testing.T) {
	for _, tc := range []struct {
		name, input string
		want        []map[string]any
	}{
		{
			"pretty object",
			"\n{\n  \"name\": \"O'Reilly\",\n  \"example\": \"{color:blue}\",\n  \"nested\": {\"enabled\": true}\n}\n",
			[]map[string]any{{"name": "O'Reilly", "example": "{color:blue}", "nested": map[string]any{"enabled": true}}},
		},
		{
			"pretty array",
			"\n[\n  {\"name\": \"O'Reilly\"},\n  {\"name\": \"D'Angelo\"}\n]\n",
			[]map[string]any{{"name": "O'Reilly"}, {"name": "D'Angelo"}},
		},
		{
			"compact object followed by blank lines",
			"\n  \n{\"name\":\"O'Reilly\",\"example\":\"{color:blue}\"}\r\n \r\n\t\r\n",
			[]map[string]any{{"name": "O'Reilly", "example": "{color:blue}"}},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// MaxRecordBytes bounds JSONL records, not a complete JSON document.
			opts := &FuzzyImportOptions{ImportOptions: &ImportOptions{MaxRecordBytes: 8}, FuzzyJSON: true}
			got, err := fuzzyParseJSONInput(tc.input, opts)
			if err != nil || !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("records=%#v, error=%v; want %#v", got, err, tc.want)
			}
		})
	}
}

func TestFuzzyJSONLinesPreserveValidRowsBeforeRepair(t *testing.T) {
	for _, separator := range []string{"\n", "\r\n", "\r"} {
		input := strings.Join([]string{
			"", "  ",
			`{"name":"O'Reilly","example":"{color:blue}","escaped":"one\ntwo"}`,
			"\t", `{name:'Ada',example:'plain'}`, "",
		}, separator)
		want := []map[string]any{
			{"name": "O'Reilly", "example": "{color:blue}", "escaped": "one\ntwo"},
			{"name": "Ada", "example": "plain"},
		}
		got, err := fuzzyParseJSONInput(input, &FuzzyImportOptions{FuzzyJSON: true})
		if err != nil || !reflect.DeepEqual(got, want) {
			t.Fatalf("separator=%q: records=%#v error=%v; want %#v", separator, got, err, want)
		}
	}
}

func TestFuzzyJSONLinesInvalidRowPolicyAndLineNumber(t *testing.T) {
	for _, separator := range []string{"\n", "\r\n", "\r"} {
		input := strings.Join([]string{"", " ", `{"id":1}`, "not JSON", `{"id":2}`, ""}, separator)
		for _, skip := range []bool{false, true} {
			opts := &FuzzyImportOptions{FuzzyJSON: true, SkipInvalidRows: skip}
			got, err := fuzzyParseJSONInput(input, opts)
			if !skip {
				if err == nil || !strings.Contains(err.Error(), "line 4:") {
					t.Fatalf("separator=%q skip=false: error=%v, want invalid JSON on line 4", separator, err)
				}
				continue
			}
			want := []map[string]any{{"id": float64(1)}, {"id": float64(2)}}
			if err != nil || !reflect.DeepEqual(got, want) {
				t.Fatalf("separator=%q skip=true: records=%#v error=%v; want %#v", separator, got, err, want)
			}
		}
	}
}

func TestFuzzyJSONLinesRecordLimit(t *testing.T) {
	longText := strings.Repeat("x", 70*1024)
	longRecord := `{"text":"` + longText + `"}`
	shortRecord := `{"text":"short"}`
	for _, separator := range []string{"\n", "\r\n", "\r"} {
		for _, longFirst := range []bool{false, true} {
			lines := []string{shortRecord, longRecord}
			if longFirst {
				lines[0], lines[1] = lines[1], lines[0]
			}
			input := strings.Join(lines, separator) + separator
			for _, skip := range []bool{false, true} {
				opts := &FuzzyImportOptions{ImportOptions: &ImportOptions{MaxRecordBytes: 64 * 1024}, FuzzyJSON: true, SkipInvalidRows: skip}
				if _, err := fuzzyParseJSONInput(input, opts); err == nil {
					t.Fatalf("separator=%q longFirst=%v skip=%v: oversized JSONL record accepted", separator, longFirst, skip)
				}
			}
			opts := &FuzzyImportOptions{ImportOptions: &ImportOptions{MaxRecordBytes: 128 * 1024}, FuzzyJSON: true}
			got, err := fuzzyParseJSONInput(input, opts)
			want := []map[string]any{{"text": "short"}, {"text": longText}}
			if longFirst {
				want[0], want[1] = want[1], want[0]
			}
			if err != nil || !reflect.DeepEqual(got, want) {
				t.Fatalf("separator=%q longFirst=%v: long JSONL import records=%d error=%v", separator, longFirst, len(got), err)
			}
		}
	}
}
