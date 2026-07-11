//go:build tinygo.wasm || baremetal

package engine

import "runtime"

// TinyGo intentionally exposes a smaller runtime.MemStats surface. Preserve
// the catalog shape and mark metrics it cannot report as unavailable.
func runtimeStatusMemoryRows(mem runtime.MemStats) [][2]string {
	return [][2]string{{"gc_runs", "unavailable"}}
}

func runtimeDetailedMemoryRows(mem runtime.MemStats) [][2]string {
	return [][2]string{
		{"stack_inuse_bytes", "unavailable"},
		{"stack_sys_bytes", "unavailable"},
		{"gc_runs", "unavailable"},
		{"gc_pause_total_ns", "unavailable"},
		{"gc_pause_total_ms", "unavailable"},
		{"gc_cpu_fraction", "unavailable"},
	}
}
