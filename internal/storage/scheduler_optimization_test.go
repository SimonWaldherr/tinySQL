package storage

import (
	"testing"
	"time"
)

func TestNextIntervalRunPreservesCadence(t *testing.T) {
	last := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name          string
		elapsed, want time.Duration
	}{
		{"before first", 3 * time.Second, 10 * time.Second},
		{"exact first", 10 * time.Second, 10 * time.Second},
		{"exact later", 100 * time.Second, 100 * time.Second},
		{"between ticks", 101 * time.Second, 110 * time.Second},
		{"clock moved back", -time.Second, 10 * time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := nextIntervalRun(last, last.Add(tc.elapsed), 10*time.Second)
			if want := last.Add(tc.want); !got.Equal(want) {
				t.Fatalf("got %v, want %v", got, want)
			}
		})
	}
	// A long outage at millisecond cadence must not loop once per missed tick.
	now := last.Add(365*24*time.Hour + time.Nanosecond)
	if got, want := nextIntervalRun(last, now, time.Millisecond), now.Add(time.Millisecond-time.Nanosecond); !got.Equal(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	// time.Sub saturates for spans longer than a time.Duration can represent.
	ancient := time.Date(1000, 1, 1, 0, 0, 0, 0, time.UTC)
	if got := nextIntervalRun(ancient, last, time.Hour); !got.Equal(last) {
		t.Fatalf("long-span deadline: %v", got)
	}
}

func TestSchedulerCronUsesConfiguredTimezone(t *testing.T) {
	for _, tc := range []struct {
		name, expr, timezone string
		now, want            time.Time
	}{
		{"summer", "0 0 9 * * *", "Europe/Berlin", time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC), time.Date(2026, 7, 1, 7, 0, 0, 0, time.UTC)},
		{"winter", "0 0 9 * * *", "Europe/Berlin", time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2026, 1, 1, 8, 0, 0, 0, time.UTC)},
		{"explicit expression wins", "CRON_TZ=Asia/Tokyo 0 0 9 * * *", "Europe/Berlin", time.Date(2026, 7, 1, 1, 0, 0, 0, time.UTC), time.Date(2026, 7, 2, 0, 0, 0, 0, time.UTC)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := NewDB()
			defer db.Close()
			s := NewScheduler(db, nil)
			job := &CatalogJob{Name: tc.name, ScheduleType: "CRON", CronExpr: tc.expr, Timezone: tc.timezone}
			if err := s.scheduleCronJob(job); err != nil {
				t.Fatal(err)
			}
			registered := s.cron.Entry(s.cronEntries[job.Name]).Schedule
			if got := registered.Next(tc.now); !got.Equal(tc.want) {
				t.Fatalf("registered next = %v, want %v", got, tc.want)
			}
			parsed, err := parseJobCronSchedule(job)
			if err != nil {
				t.Fatal(err)
			}
			if got := parsed.Next(tc.now); !got.Equal(tc.want) {
				t.Fatalf("calculated next = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestSchedulerRejectsOverflowingInterval(t *testing.T) {
	s := &Scheduler{}
	last := time.Now().Add(-time.Hour)
	job := &CatalogJob{Name: "overflow", ScheduleType: "INTERVAL", IntervalMs: 1<<63 - 1, LastRunAt: &last, CatchUp: true}
	s.calculateNextRun(job)
	if job.NextRunAt != nil {
		t.Fatalf("overflowing interval produced deadline %v", job.NextRunAt)
	}
}
