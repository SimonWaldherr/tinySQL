package storage

import (
	"strconv"
	"testing"
	"time"
)

func BenchmarkSchedulerIdleIntervalPoll(b *testing.B) {
	db := NewDB()
	b.Cleanup(func() { db.Close() })
	s := NewScheduler(db, nil)
	now := time.Now()
	future := now.Add(time.Hour)
	for i := range 10000 {
		kind := "INTERVAL"
		if i%2 == 0 {
			kind = "CRON"
		}
		if err := s.catalog.RegisterJob(&CatalogJob{
			Name: "job-" + strconv.Itoa(i), Enabled: true,
			ScheduleType: kind, NextRunAt: &future,
		}); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
	for b.Loop() {
		s.checkIntervalJobs(now)
	}
}

func BenchmarkSchedulerCatchUp(b *testing.B) {
	s := &Scheduler{}
	last := time.Now().Add(-24 * time.Hour)
	job := &CatalogJob{ScheduleType: "INTERVAL", IntervalMs: 100, LastRunAt: &last, CatchUp: true}
	b.ReportAllocs()
	for b.Loop() {
		s.calculateNextRun(job)
	}
}

func BenchmarkSchedulerStopIdle(b *testing.B) {
	db := NewDB()
	b.Cleanup(func() { db.Close() })
	b.ReportAllocs()
	for b.Loop() {
		b.StopTimer()
		s := NewScheduler(db, nil)
		b.StartTimer()
		s.Stop()
	}
}
