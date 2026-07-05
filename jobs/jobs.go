// Package jobs provides validated constructors for tinySQL catalog jobs.
package jobs

import (
	"fmt"
	"strings"
	"time"

	tinysql "github.com/SimonWaldherr/tinySQL"
)

const defaultMaxRuntimeMs int64 = 60000

// Config describes a scheduled tinySQL job before validation.
type Config struct {
	Name         string
	SQL          string
	ScheduleType string
	CronExpr     string
	IntervalMs   int64
	RunAt        *time.Time
	Timezone     string
	Enabled      *bool
	CatchUp      bool
	NoOverlap    bool
	MaxRuntimeMs int64
}

// Build validates cfg and returns a CatalogJob.
func Build(cfg Config) (*tinysql.CatalogJob, error) {
	name := strings.TrimSpace(cfg.Name)
	sqlText := strings.TrimSpace(cfg.SQL)
	scheduleType := strings.ToUpper(strings.TrimSpace(cfg.ScheduleType))
	if scheduleType == "" {
		scheduleType = "INTERVAL"
	}
	if name == "" {
		return nil, fmt.Errorf("job name is required")
	}
	if sqlText == "" {
		return nil, fmt.Errorf("job sql is required")
	}
	maxRuntimeMs := cfg.MaxRuntimeMs
	if maxRuntimeMs <= 0 {
		maxRuntimeMs = defaultMaxRuntimeMs
	}
	now := time.Now()
	job := &tinysql.CatalogJob{
		Name:         name,
		SQLText:      sqlText,
		ScheduleType: scheduleType,
		CronExpr:     strings.TrimSpace(cfg.CronExpr),
		IntervalMs:   cfg.IntervalMs,
		Timezone:     strings.TrimSpace(cfg.Timezone),
		Enabled:      boolDefault(cfg.Enabled, true),
		CatchUp:      cfg.CatchUp,
		NoOverlap:    cfg.NoOverlap,
		MaxRuntimeMs: maxRuntimeMs,
		CreatedAt:    now,
		UpdatedAt:    now,
	}
	switch scheduleType {
	case "INTERVAL":
		if job.IntervalMs <= 0 {
			return nil, fmt.Errorf("interval_ms is required for INTERVAL jobs")
		}
	case "CRON":
		if job.CronExpr == "" {
			return nil, fmt.Errorf("cron_expr is required for CRON jobs")
		}
	case "ONCE":
		if cfg.RunAt == nil || cfg.RunAt.IsZero() {
			return nil, fmt.Errorf("run_at is required for ONCE jobs")
		}
		runAt := *cfg.RunAt
		job.RunAt = &runAt
	default:
		return nil, fmt.Errorf("unsupported schedule_type %q", scheduleType)
	}
	return job, nil
}

// NewInterval creates an INTERVAL job.
func NewInterval(name, sqlText string, interval time.Duration, enabled bool) (*tinysql.CatalogJob, error) {
	return Build(Config{
		Name:         name,
		SQL:          sqlText,
		ScheduleType: "INTERVAL",
		IntervalMs:   interval.Milliseconds(),
		Enabled:      &enabled,
	})
}

// NewCron creates a CRON job.
func NewCron(name, sqlText, cronExpr, timezone string, enabled bool) (*tinysql.CatalogJob, error) {
	return Build(Config{
		Name:         name,
		SQL:          sqlText,
		ScheduleType: "CRON",
		CronExpr:     cronExpr,
		Timezone:     timezone,
		Enabled:      &enabled,
	})
}

// NewOnce creates a one-shot job.
func NewOnce(name, sqlText string, runAt time.Time, enabled bool) (*tinysql.CatalogJob, error) {
	return Build(Config{
		Name:         name,
		SQL:          sqlText,
		ScheduleType: "ONCE",
		RunAt:        &runAt,
		Enabled:      &enabled,
	})
}

func boolDefault(v *bool, fallback bool) bool {
	if v == nil {
		return fallback
	}
	return *v
}
