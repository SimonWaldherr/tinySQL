package storage

import (
	"encoding/json"
	"fmt"
	"math/big"
	"testing"
	"time"
)

func BenchmarkJSONMarshalNormalization(b *testing.B) {
	for _, tc := range []struct {
		name    string
		special bool
		custom  bool
	}{{"ordinary", false, false}, {"mixed", true, false}, {"custom", false, true}} {
		b.Run(tc.name, func(b *testing.B) {
			rows := make([]any, 1000)
			for i := range rows {
				var metric any = 1.25
				if tc.special && i%10 == 0 {
					metric = big.NewRat(int64(i+1), 7)
				}
				if tc.custom && i%10 == 0 {
					metric = time.Date(2026, 11, 7, 0, 0, 0, 0, time.UTC)
				}
				rows[i] = map[string]any{"id": i, "name": fmt.Sprintf("row-%d", i), "metrics": []any{metric, true, nil, map[string]any{"region": "north"}}}
			}
			for _, version := range []struct {
				name    string
				marshal func(any) ([]byte, error)
			}{
				{"eager", func(v any) ([]byte, error) { return json.Marshal(eagerJSONNormalize(v)) }},
				{"on_change", JSONMarshal},
			} {
				b.Run(version.name, func(b *testing.B) {
					b.ReportAllocs()
					for b.Loop() {
						if _, err := version.marshal(rows); err != nil {
							b.Fatal(err)
						}
					}
				})
			}
		})
	}
}

var benchmarkJSONRows [][]any

func BenchmarkJSONDiskRowNormalization(b *testing.B) {
	rows := benchTable("normalization", 1000).Rows
	b.Run("eager", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			normalized := make([][]any, len(rows))
			for i, row := range rows {
				normalized[i] = eagerJSONNormalize(row).([]any)
			}
			benchmarkJSONRows = normalized
		}
	})
	b.Run("on_change", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			benchmarkJSONRows = normalizeJSONRows(rows)
		}
	})
}
