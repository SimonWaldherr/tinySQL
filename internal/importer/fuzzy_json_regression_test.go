package importer

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestFuzzyImportJSONPreservesValidStrings(t *testing.T) {
	for _, tc := range []struct {
		name, input string
		want        []string
	}{
		{"object", `{"name":"O'Reilly", "example":"{color:blue}"}`, []string{"O'Reilly"}},
		{"array", `[{"name":"O'Reilly"},{"name":"D'Angelo"}]`, []string{"O'Reilly", "D'Angelo"}},
		{"jsonl", "{\"name\":\"O'Reilly\"}\n{\"name\":\"D'Angelo\"}\n", []string{"O'Reilly", "D'Angelo"}},
		{"jsonl CR", "{\"name\":\"Ada\"}\r{\"name\":\"Grace\"}\r", []string{"Ada", "Grace"}},
		{"fuzzy object", `{name:'Ada'}`, []string{"Ada"}},
		{"fuzzy array", `[{name:'Ada'},{name:'Grace'}]`, []string{"Ada", "Grace"}},
		{"mixed jsonl", "{\"name\":\"O'Reilly\"}\n{name:'Ada'}\n", []string{"O'Reilly", "Ada"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := storage.NewDB()
			opts := &FuzzyImportOptions{ImportOptions: &ImportOptions{CreateTable: true}, FuzzyJSON: true}
			result, err := FuzzyImportJSON(context.Background(), db, "default", "records", strings.NewReader(tc.input), opts)
			if err != nil || result.RowsInserted != int64(len(tc.want)) {
				t.Fatalf("import = %#v, error = %v", result, err)
			}
			table, err := db.Get("default", "records")
			if err != nil {
				t.Fatal(err)
			}
			idx, err := table.ColIndex("name")
			if err != nil {
				t.Fatal(err)
			}
			got := make([]string, len(table.Rows))
			for i, row := range table.Rows {
				got[i], _ = row[idx].(string)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("names = %#v, want %#v", got, tc.want)
			}
			if tc.name == "object" {
				idx, err := table.ColIndex("example")
				if err != nil || table.Rows[0][idx] != "{color:blue}" {
					t.Fatalf("example was changed: %#v, error %v", table.Rows[0], err)
				}
			}
		})
	}
}

func TestFuzzyImportJSONHonorsInputAndRecordLimits(t *testing.T) {
	longValue := strings.Repeat("x", 100*1024)
	input := `{"text":"` + longValue + `"}` + "\n" + `{"text":"second"}` + "\n"
	newOptions := func(inputLimit int64, recordLimit int) *FuzzyImportOptions {
		return &FuzzyImportOptions{ImportOptions: &ImportOptions{
			CreateTable: true, MaxInputBytes: inputLimit, MaxRecordBytes: recordLimit,
		}}
	}
	db := storage.NewDB()
	result, err := FuzzyImportJSON(context.Background(), db, "default", "records", strings.NewReader(input), newOptions(int64(len(input)), len(input)))
	if err != nil || result.RowsInserted != 2 {
		t.Fatalf("long JSONL import = %#v, error %v", result, err)
	}
	table, err := db.Get("default", "records")
	if err != nil {
		t.Fatal(err)
	}
	idx, err := table.ColIndex("text")
	if err != nil || table.Rows[0][idx] != longValue {
		t.Fatalf("long record changed, error %v", err)
	}
	if _, err := FuzzyImportJSON(context.Background(), storage.NewDB(), "default", "records", strings.NewReader(input), newOptions(int64(len(input)-1), len(input))); !errors.Is(err, ErrInputTooLarge) {
		t.Fatalf("oversized input error = %v, want ErrInputTooLarge", err)
	}
	if _, err := FuzzyImportJSON(context.Background(), storage.NewDB(), "default", "records", strings.NewReader(input), newOptions(int64(len(input)), 64*1024)); err == nil {
		t.Fatal("oversized JSONL record was accepted")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := FuzzyImportJSON(ctx, storage.NewDB(), "default", "records", strings.NewReader(input), newOptions(int64(len(input)), len(input))); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled input error = %v, want context.Canceled", err)
	}
}

func TestFuzzyImportJSONWithNilEmbeddedOptions(t *testing.T) {
	db := storage.NewDB()
	table := storage.NewTable("records", []storage.Column{{Name: "name", Type: storage.TextType}}, false)
	if err := db.Put("default", table); err != nil {
		t.Fatal(err)
	}
	opts := &FuzzyImportOptions{}
	result, err := FuzzyImportJSON(context.Background(), db, "default", "records", strings.NewReader(`{"name":"Ada"}`), opts)
	if err != nil || result.RowsInserted != 1 || opts.ImportOptions == nil {
		t.Fatalf("import = %#v, options = %#v, error = %v", result, opts, err)
	}
}
