package importer

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestImportCSVStreamingErrors(t *testing.T) {
	input := "id\n" + strings.Repeat("1\n", 20000)
	for _, tc := range []struct {
		name   string
		source io.Reader
		opts   ImportOptions
		want   error
	}{
		{"limit during sampling", strings.NewReader(input), ImportOptions{SampleRecords: 20000, MaxInputBytes: 5000}, ErrInputTooLarge},
		{"limit during insert", strings.NewReader(input), ImportOptions{SampleRecords: 1, MaxInputBytes: 5000}, ErrInputTooLarge},
		{"source error", io.MultiReader(strings.NewReader(input), failingCSVReader{}), ImportOptions{SampleRecords: 1}, errCSVSource},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tc.opts.HeaderMode = "present"
			_, err := ImportCSV(context.Background(), storage.NewDB(), "default", "records", tc.source, &tc.opts)
			if !errors.Is(err, tc.want) {
				t.Fatalf("ImportCSV error = %v, want %v", err, tc.want)
			}
		})
	}
}

var errCSVSource = errors.New("source failed")

type failingCSVReader struct{}

func (failingCSVReader) Read([]byte) (int, error) { return 0, errCSVSource }

type finalChunkReader struct{ done bool }

func (r *finalChunkReader) Read(p []byte) (int, error) {
	if r.done {
		return 0, io.EOF
	}
	r.done = true
	return copy(p, []byte{'A', 0xff}), io.EOF
}

func TestStrictUTF8ReaderRejectsInvalidFinalChunk(t *testing.T) {
	_, err := io.ReadAll(newStrictUTF8Reader(&finalChunkReader{}))
	if err == nil || !strings.Contains(err.Error(), "invalid UTF-8") {
		t.Fatalf("error = %v, want invalid UTF-8", err)
	}
}

func TestImportCSVRejectsInvalidUTF16Surrogates(t *testing.T) {
	for _, data := range [][]byte{
		{0xff, 0xfe, 'x', 0, '\n', 0, 0x00, 0xd8},
		{0xfe, 0xff, 0, 'x', 0, '\n', 0xdc, 0x00},
	} {
		_, err := ImportCSV(context.Background(), storage.NewDB(), "default", "invalid", bytes.NewReader(data), nil)
		if err == nil || !strings.Contains(err.Error(), "invalid UTF-16 surrogate") {
			t.Fatalf("input %x: error = %v, want invalid UTF-16", data, err)
		}
	}
	decoded, err := decodeUTF16All([]byte{0xff, 0xfe, 0x3d, 0xd8, 0x00, 0xde}, false)
	if err != nil || string(decoded) != "😀" {
		t.Fatalf("valid surrogate pair: decoded = %q, error = %v", decoded, err)
	}
}

func TestImportCSVStrictTypesSkipsInvalidRow(t *testing.T) {
	db := storage.NewDB()
	result, err := ImportCSV(context.Background(), db, "default", "records", strings.NewReader("id\n1\ninvalid\n3\n"), &ImportOptions{
		HeaderMode: "present", StrictTypes: true, SampleRecords: 1, BatchSize: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.RowsInserted != 2 || result.RowsSkipped != 1 {
		t.Fatalf("inserted/skipped = %d/%d, errors = %v", result.RowsInserted, result.RowsSkipped, result.Errors)
	}
	table, err := db.Get("default", "records")
	if err != nil {
		t.Fatal(err)
	}
	if len(table.Rows) != 2 || fmt.Sprint(table.Rows[1][0]) != "3" {
		t.Fatalf("rows = %v", table.Rows)
	}
}
