package storage

import (
	"fmt"
	"testing"
)

var rowUpdateHistorySink rowUpdateHistory

func BenchmarkRowUpdateHistoryClone(b *testing.B) {
	for _, count := range []int{0, 1, 63, 64, 65, 1024, 4095, 4096} {
		history, _ := makeHistoryTestData(count)
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				rowUpdateHistorySink = history.clone()
			}
		})
	}
}

func BenchmarkRowUpdateHistoryCloneAppend(b *testing.B) {
	for _, count := range []int{0, 1, 63, 64, 65, 1024, 4095, 4096} {
		history, model := makeHistoryTestData(count)
		version := 100
		if len(model) != 0 {
			version = model[len(model)-1].structVersion + 1
		}
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				next := history.clone()
				next.add(rowUpdateDelta{structVersion: version, row: 7})
				rowUpdateHistorySink = next
			}
		})
	}
}
