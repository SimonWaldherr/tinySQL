package engine

import (
	"errors"
	"sync/atomic"
)

var sqlHostIODisabled atomic.Bool
var errSQLHostIODisabled = errors.New("SQL host I/O is disabled (FILE and HTTP)")

// DisableSQLHostIO disables the FILE and HTTP scalar functions process-wide.
func DisableSQLHostIO() {
	sqlHostIODisabled.Store(true)
}
