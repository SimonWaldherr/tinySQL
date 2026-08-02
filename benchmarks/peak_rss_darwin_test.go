//go:build darwin

package benchmarks

import "golang.org/x/sys/unix"

func benchmarkPeakRSSBytes() uint64 {
	var usage unix.Rusage
	if err := unix.Getrusage(unix.RUSAGE_SELF, &usage); err != nil || usage.Maxrss <= 0 {
		return 0
	}
	// Darwin reports ru_maxrss in bytes.
	return uint64(usage.Maxrss)
}
