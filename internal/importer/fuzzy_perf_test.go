package importer

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestFuzzyDelimiterEarlyExitMatchesFullScan(t *testing.T) {
	for _, tc := range []struct {
		name       string
		input      string
		candidates []rune
	}{
		{"comma-quoted-newline", "name,description\nAlice,\"line one\nline two; extra\"\nBob,\"quoted \"\"value\"\"\"\n", nil},
		{"semicolon-bom-crlf", "\ufeffname;description\r\nAda;\"x,y\" \t\r\nBob;end\r\n", nil},
		{"tab", "id\tname\n1\talpha\n2\tbeta\n", nil},
		{"pipe", "id|name\n1|alpha\n2|beta\n", nil},
		{"equal-score-first-wins", "a,b;c,d\n1,2;3,4\n", []rune{';', ','}},
		// Two-column records must have a unique mode: the scorer iterates a
		// map, so a tie between field counts would make the reference scan
		// itself nondeterministic.
		{"inconsistent-counts", "id,name\n1,Ada\n2,Grace\n3\n4,Lin,extra\n", nil},
		{"single-column", "name\nAda\nGrace\n", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			opts := &FuzzyImportOptions{ImportOptions: &ImportOptions{DelimiterCandidates: tc.candidates}, FixQuotes: true, RemoveInvalidChars: true}
			cleaned, err := cleanInputData(strings.NewReader(tc.input), opts)
			if err != nil {
				t.Fatal(err)
			}
			gotDelimiter, gotRecords, gotErr := fuzzyParseCSV(cleaned, opts)
			candidates := tc.candidates
			if len(candidates) == 0 {
				candidates = []rune{',', ';', '\t', '|'}
			}
			var wantDelimiter rune
			var wantRecords [][]string
			var bestScore float64
			for _, delim := range candidates {
				records, score := tryParseWithDelimiter(cleaned, delim, opts)
				if score > bestScore && len(records) > 0 {
					wantDelimiter, wantRecords, bestScore = delim, records, score
				}
			}
			if gotErr != nil || gotDelimiter != wantDelimiter || !reflect.DeepEqual(gotRecords, wantRecords) {
				t.Fatalf("got delimiter %q records %#v error %v; full scan wanted %q %#v", gotDelimiter, gotRecords, gotErr, wantDelimiter, wantRecords)
			}
		})
	}
}

func BenchmarkFuzzyImportCSV(b *testing.B) {
	var data strings.Builder
	data.WriteString("id,name,city,description\n")
	for i := 0; i < 10000; i++ {
		fmt.Fprintf(&data, "%d,name %d,Berlin,\"hello, world\"\n", i, i)
	}
	input := data.String()
	ctx := context.Background()
	b.ReportAllocs()
	b.SetBytes(int64(len(input)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		opts := &FuzzyImportOptions{ImportOptions: &ImportOptions{CreateTable: true, TypeInference: true, HeaderMode: "auto"}}
		if result, err := FuzzyImportCSV(ctx, storage.NewDB(), "default", "items", strings.NewReader(input), opts); err != nil || result.RowsInserted != 10000 {
			b.Fatalf("import result: %#v; error: %v", result, err)
		}
	}
}

func BenchmarkFuzzyParseCSVDelimiters(b *testing.B) {
	for _, tc := range []struct {
		name      string
		delimiter string
	}{
		{"comma", ","}, {"semicolon", ";"}, {"tab", "\t"}, {"pipe", "|"},
	} {
		var data strings.Builder
		fmt.Fprintf(&data, "id%sname%scity%scomment\n", tc.delimiter, tc.delimiter, tc.delimiter)
		for i := 0; i < 10000; i++ {
			fmt.Fprintf(&data, "%d%sname %d%sBerlin%s\"hello, world\"\n", i, tc.delimiter, i, tc.delimiter, tc.delimiter)
		}
		input := data.String()
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(input)))
			for i := 0; i < b.N; i++ {
				if _, _, err := fuzzyParseCSV(input, &FuzzyImportOptions{ImportOptions: &ImportOptions{}}); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
