package storage

import (
	"testing"
	"time"
)

func BenchmarkSchedulerCatchUp(b *testing.B) {
	s := &Scheduler{}
	last := time.Now().Add(-24 * time.Hour)
	job := &CatalogJob{ScheduleType: "INTERVAL", IntervalMs: 100, LastRunAt: &last, CatchUp: true}
	b.ReportAllocs()
	for b.Loop() {
		s.calculateNextRun(job)
	}
}
