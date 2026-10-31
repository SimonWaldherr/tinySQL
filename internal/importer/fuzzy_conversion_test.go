package importer

import (
	"strings"
	"testing"
)

func TestRemoveNonNumericPreservesFuzzyFormatting(t *testing.T) {
	for _, tc := range []struct{ input, integer, decimal string }{
		{"", "", ""},
		{"12345", "12345", "12345"},
		{"+123", "+123", "+123"},
		{"+", "+", "+"},
		{"-.", "-", "-."},
		{"-12.50", "-1250", "-12.50"},
		{"1,234.56", "123456", "1.23456"},
		{"1e3", "13", "13"},
		{"-.5", "-5", "-.5"},
		{"--12", "-12", "-12"},
		{" + 12", "+12", "+12"},
		{"1.2.3", "123", "1.23"},
		{"１２.٣", "１２٣", "１２.٣"},
		{"\xff12", "12", "12"},
		{".", "", "."},
	} {
		for _, decimal := range []bool{false, true} {
			want := tc.integer
			if decimal {
				want = tc.decimal
			}
			if got := removeNonNumeric(tc.input, decimal); got != want {
				t.Errorf("input=%q decimal=%v: got %q, want %q", tc.input, decimal, got, want)
			}
		}
	}
}

func BenchmarkRemoveNonNumeric(b *testing.B) {
	for _, tc := range []struct {
		name, value, want string
		decimal           bool
	}{
		{"integer", "123456", "123456", false},
		{"signed", "-123456", "-123456", false},
		{"decimal", "1234.50", "1234.50", true},
		{"formatted", "$1,234.50", "1.23450", true},
	} {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				if got := removeNonNumeric(tc.value, tc.decimal); got != tc.want {
					b.Fatalf("got %q, want %q", got, tc.want)
				}
			}
		})
	}
}

func BenchmarkFuzzyParseJSONLinesRepair(b *testing.B) {
	input := strings.Repeat("{id: 1, name: 'Ada'}\n", 1000)
	opts := &FuzzyImportOptions{FuzzyJSON: true}
	b.SetBytes(int64(len(input)))
	b.ReportAllocs()
	for b.Loop() {
		records, err := fuzzyParseJSONInput(input, opts)
		if err != nil || len(records) != 1000 || records[999]["name"] != "Ada" {
			b.Fatalf("records=%d error=%v", len(records), err)
		}
	}
}
