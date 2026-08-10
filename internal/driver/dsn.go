// The DSN: what a connection string can say and how it is parsed. Unknown or
// malformed options are errors rather than silently ignored defaults, so a typo
// in a durability setting cannot quietly downgrade it.
package driver

import (
	"fmt"
	"net/url"
	"path/filepath"
	"strings"
	"time"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// cfg stores the connection parameters derived from a parsed DSN.
type cfg struct {
	// defaultDSN is true only for the empty DSN used by the legacy embedding
	// helpers. Named mem:// and file: DSNs must never inherit SetDefaultDB.
	defaultDSN  bool
	tenant      string
	filePath    string
	autosave    bool
	maxReaders  int
	maxWriters  int
	busyTimeout time.Duration
	// mode selects a storage.StorageMode other than the driver's original
	// in-memory-plus-GOB-snapshot behavior (e.g. "disk", "json", "wal").
	// modeSet distinguishes "not specified" (keep the original LoadFromFile/
	// NewDB + autosave path, unchanged) from an explicit "memory" (which
	// behaves the same but goes through storage.OpenDB).
	mode    storage.StorageMode
	modeSet bool

	maxMemoryBytes     int64
	readOnly           bool
	syncOnMutate       bool
	compressFiles      bool
	checkpointEvery    uint64
	checkpointInterval time.Duration
	checkpointMaxBytes int64

	// persistDebounce is OFF by default (zero value): every write-shaped
	// statement's persist() call performs its durable sync synchronously and
	// immediately, exactly as before this option existed. When set to a
	// positive duration via the persist_debounce_ms DSN option, persist()
	// instead coalesces a burst of rapid statements into at most one actual
	// sync per debounce window — see server.persist's doc comment for the
	// durability tradeoff this introduces.
	persistDebounce time.Duration
}

// parseDSN parses a tinySQL DSN into a driver configuration.
func parseDSN(dsn string) (cfg, error) {
	var c cfg
	c.tenant = "default"
	c.maxWriters = 1
	if dsn == "" {
		c.defaultDSN = true
		return c, nil
	}
	switch {
	case strings.HasPrefix(dsn, "mem://"):
		if i := strings.Index(dsn, "?"); i >= 0 {
			if err := applyQueryOptions(dsn[i+1:], &c); err != nil {
				return c, err
			}
		}
		return c, nil
	case strings.HasPrefix(dsn, "file:"):
		path := strings.TrimPrefix(dsn, "file:")
		q := ""
		if i := strings.Index(path, "?"); i >= 0 {
			q = path[i+1:]
			path = path[:i]
		}
		if path == "" {
			return c, fmt.Errorf("file: path required")
		}
		c.filePath = filepath.Clean(path)
		if q != "" {
			if err := applyQueryOptions(q, &c); err != nil {
				return c, err
			}
		}
		return c, nil
	default:
		return c, fmt.Errorf("unsupported DSN")
	}
}

// applyQueryOptions parses a URL-style query string (k=v&k2=v2) and applies
// options to the provided cfg using applyDSNOption. This consolidates repeated
// logic used for different DSN prefixes (mem:// and file:).
func applyQueryOptions(q string, c *cfg) error {
	values, err := url.ParseQuery(q)
	if err != nil {
		return fmt.Errorf("tinysql: invalid DSN query: %w", err)
	}
	for key, values := range values {
		if len(values) != 1 {
			return fmt.Errorf("tinysql: DSN option %q must occur once", key)
		}
		if err := applyDSNOption(c, key, values[0]); err != nil {
			return err
		}
	}
	return nil
}
