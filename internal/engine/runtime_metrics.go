//go:build !tinygo.wasm && !baremetal

package engine

import (
	"fmt"
	"runtime"
)

func runtimeStatusMemoryRows(mem runtime.MemStats) [][2]string {
	return [][2]string{{"gc_runs", fmt.Sprintf("%d", mem.NumGC)}}
}

func runtimeDetailedMemoryRows(mem runtime.MemStats) [][2]string {
	return [][2]string{
		{"stack_inuse_bytes", fmt.Sprintf("%d", mem.StackInuse)},
		{"stack_sys_bytes", fmt.Sprintf("%d", mem.StackSys)},
		{"gc_runs", fmt.Sprintf("%d", mem.NumGC)},
		{"gc_pause_total_ns", fmt.Sprintf("%d", mem.PauseTotalNs)},
		{"gc_pause_total_ms", fmt.Sprintf("%.2f", float64(mem.PauseTotalNs)/1e6)},
		{"gc_cpu_fraction", fmt.Sprintf("%.6f", mem.GCCPUFraction)},
	}
}
