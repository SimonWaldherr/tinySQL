//go:build !darwin && !linux

package benchmarks

func benchmarkPeakRSSBytes() uint64 { return 0 }
