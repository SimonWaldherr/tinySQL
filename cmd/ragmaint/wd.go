package main

import (
	"fmt"
	"os"
	"runtime/metrics"
	"strings"
	"time"
)

// watchdog aborts the process when Go's total mapped memory exceeds capGB, to
// protect the operator's laptop.
func watchdog(capGB float64) {
	if capGB <= 0 {
		return
	}
	go func() {
		s := []metrics.Sample{{Name: "/memory/classes/total:bytes"}}
		for {
			time.Sleep(500 * time.Millisecond)
			metrics.Read(s)
			if gb := float64(s[0].Value.Uint64()) / (1 << 30); gb > capGB {
				fmt.Printf("WATCHDOG: Go memory %.1f GiB exceeds cap %.1f GiB, aborting\n", gb, capGB)
				os.Exit(3)
			}
		}
	}()
}

func mutSelected(name string) bool {
	has := func(list string) bool {
		for _, s := range splitCSV(list) {
			if s != "" && strings.Contains(name, s) {
				return true
			}
		}
		return false
	}
	if *onlyMut != "" && !has(*onlyMut) {
		return false
	}
	if *skipMut != "" && has(*skipMut) {
		return false
	}
	return true
}

func splitCSV(s string) []string { return strings.Split(s, ",") }
