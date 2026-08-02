//go:build linux

package benchmarks

import "golang.org/x/sys/unix"

func benchmarkPeakRSSBytes() uint64 {
	var usage unix.Rusage
	if err := unix.Getrusage(unix.RUSAGE_SELF, &usage); err != nil || usage.Maxrss <= 0 {
		return 0
	}
	// Linux reports ru_maxrss in KiB.
	return uint64(usage.Maxrss) * 1024
}
