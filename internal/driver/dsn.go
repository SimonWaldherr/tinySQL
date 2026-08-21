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
	// walSync is the per-commit flush strength used by WAL storage modes.
	// Its zero value is WALSyncFull, preserving historical durability for
	// callers that do not opt in to the SQLite-compatible normal policy.
	walSync storage.WALSyncMode

	// persistDebounce is OFF by default (zero value): every write-shaped
	// statement's persist() call performs its durable sync synchronously and
	// immediately, exactly as before this option existed. When set to a
	// positive duration via the persist_debounce_ms DSN option, persist()
	// instead coalesces a burst of rapid statements into at most one actual
	// sync per debounce window — see server.persist's doc comment for the
	// durability tradeoff this introduces.
	persistDebounce time.Duration
}

// memoryDSNTarget is SQLite's spelling for "no file, keep it in RAM". tinySQL
// accepts it as a synonym for mem:// so a project migrating off a Go SQLite
// driver does not have to rewrite its connection string to try tinySQL.
const memoryDSNTarget = ":memory:"

// parseDSN parses a tinySQL DSN into a driver configuration.
//
// Accepted forms:
//
//	""                     the legacy empty DSN (SetDefaultDB embedding path)
//	mem://[?opts]          in-memory
//	:memory:[?opts]        in-memory, SQLite's spelling
//	file:PATH[?opts]       file-backed
//	file::memory:[?opts]   in-memory, SQLite's URI spelling
//	PATH[?opts]            file-backed; a bare path with no scheme at all
//
// The scheme is compared case-insensitively, so FILE: and MEM:// work.
//
// A bare path is accepted because that is what every Go SQLite driver takes and
// therefore what a migrant types first. The deliberate limit is that only a
// string with *no scheme* is a path: an explicit but unrecognized scheme
// (`men://x`, a typo for mem://) is an error, never a file named "men://x".
// Silently creating that file would turn a one-character typo into a database
// the caller can never find again.
func parseDSN(dsn string) (cfg, error) {
	var c cfg
	c.tenant = "default"
	c.maxWriters = 1
	if dsn == "" {
		c.defaultDSN = true
		return c, nil
	}

	body, query := splitDSNQuery(dsn)
	scheme, target := splitDSNScheme(body)

	switch scheme {
	case "mem":
		// mem:// carries no path; anything after it is ignored as it always was.
	case "file":
		// file::memory: is aliased to in-memory rather than rejected or taken
		// literally. Taken literally it used to create a real file *named*
		// ":memory:" — silently, so a caller who meant "no file" got an
		// undeletable-looking artifact on POSIX and an outright invalid path on
		// Windows, where ':' cannot appear in a filename. Both Go SQLite
		// drivers (and SQLite itself with URI filenames enabled) read this as
		// in-memory, so aliasing is the only reading that can be intended.
		if strings.EqualFold(target, memoryDSNTarget) {
			break
		}
		if target == "" {
			return c, fmt.Errorf("file: path required")
		}
		c.filePath = filepath.Clean(target)
	case "":
		if strings.EqualFold(target, memoryDSNTarget) {
			break
		}
		c.filePath = filepath.Clean(target)
	default:
		return c, fmt.Errorf("unsupported DSN scheme %q (want mem://, file:, %s, or a filesystem path)", scheme, memoryDSNTarget)
	}

	if query != "" {
		if err := applyQueryOptions(query, &c); err != nil {
			return c, err
		}
	}
	return c, nil
}

// splitDSNQuery separates the option query from the rest of the DSN. It splits
// on the first '?' because neither a scheme nor a filesystem path this driver
// can open may contain one ('?' is an illegal character in Windows paths and
// would be an option separator on every other form).
func splitDSNQuery(dsn string) (body, query string) {
	if i := strings.Index(dsn, "?"); i >= 0 {
		return dsn[:i], dsn[i+1:]
	}
	return dsn, ""
}

// splitDSNScheme separates an explicit "scheme:" prefix from the rest of a DSN,
// lower-casing the scheme. It returns an empty scheme when there is no scheme at
// all, which is what makes a bare filesystem path a valid DSN.
//
// A scheme must be at least two characters. That one rule is what keeps a
// Windows drive letter from being read as a scheme: `C:/tmp/app.db` would
// otherwise parse as the unknown scheme "c" and be rejected, and no real scheme
// is a single letter. Leading "//" after the colon is dropped so file:///tmp/x
// and mem:// both reduce to their target.
func splitDSNScheme(dsn string) (scheme, rest string) {
	i := 0
	for i < len(dsn) {
		ch := dsn[i]
		isAlpha := (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z')
		isSchemeTail := i > 0 && ((ch >= '0' && ch <= '9') || ch == '+' || ch == '-' || ch == '.')
		if !isAlpha && !isSchemeTail {
			break
		}
		i++
	}
	if i < 2 || i >= len(dsn) || dsn[i] != ':' {
		return "", dsn
	}
	return strings.ToLower(dsn[:i]), strings.TrimPrefix(dsn[i+1:], "//")
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
