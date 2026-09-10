package tinysql

import (
	"context"
	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

type SchedulerStats = storage.SchedulerStats
type JobRunInfo = storage.JobRunInfo

var ErrJobQueueFull = storage.ErrJobQueueFull
var ErrSchedulerStopped = storage.ErrSchedulerStopped
var ErrJobAlreadyRunning = storage.ErrJobAlreadyRunning

func JobRunFromContext(ctx context.Context) (JobRunInfo, bool) { return storage.JobRunFromContext(ctx) }
