// Applying one DSN option, and the parsers for the value forms options use.
package driver

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// applyDSNOption mutates the configuration in place for one URL-query option.
// Unknown or malformed options are errors: silently accepting a memory or
// durability setting is dangerous because callers believe a bound exists when
// it does not.
func applyDSNOption(c *cfg, key, value string) error {
	key = strings.ToLower(strings.TrimSpace(key))
	switch key {
	case "tenant":
		value = strings.TrimSpace(value)
		if value == "" {
			return fmt.Errorf("tinysql: tenant must not be empty")
		}
		c.tenant = value
	case "autosave":
		v, err := parseDSNBool(value, key)
		if err != nil {
			return err
		}
		c.autosave = v
	case "pool_readers", "read_pool", "reader_pool":
		n, err := parsePoolSize(value, "pool_readers")
		if err != nil {
			return err
		}
		c.maxReaders = n
	case "pool_writers", "write_pool", "writer_pool":
		n, err := parsePoolSize(value, "pool_writers")
		if err != nil {
			return err
		}
		c.maxWriters = n
	case "busy_timeout", "busytimeout":
		dur, err := parseBusyTimeout(value)
		if err != nil {
			return err
		}
		c.busyTimeout = dur
	case "mode":
		m, err := storage.ParseStorageMode(value)
		if err != nil {
			return err
		}
		c.mode = m
		c.modeSet = true
	case "max_memory_bytes":
		sz, err := parseByteSize(value, key, false)
		if err != nil {
			return err
		}
		c.maxMemoryBytes = sz
	case "read_only":
		v, err := parseDSNBool(value, key)
		if err != nil {
			return err
		}
		c.readOnly = v
	case "sync_on_mutate":
		v, err := parseDSNBool(value, key)
		if err != nil {
			return err
		}
		c.syncOnMutate = v
	case "compress_files":
		v, err := parseDSNBool(value, key)
		if err != nil {
			return err
		}
		c.compressFiles = v
	case "checkpoint_every":
		v, err := parseNonNegativeUint(value, key)
		if err != nil {
			return err
		}
		c.checkpointEvery = v
	case "checkpoint_interval":
		d, err := parseNonNegativeDuration(value, key)
		if err != nil {
			return err
		}
		c.checkpointInterval = d
	case "checkpoint_max_bytes":
		sz, err := parseByteSize(value, key, true)
		if err != nil {
			return err
		}
		c.checkpointMaxBytes = sz
	case "wal_sync":
		mode, err := storage.ParseWALSyncMode(value)
		if err != nil {
			return fmt.Errorf("tinysql: %w", err)
		}
		c.walSync = mode
	case "persist_debounce_ms":
		ms, err := parseNonNegativeUint(value, key)
		if err != nil {
			return err
		}
		c.persistDebounce = time.Duration(ms) * time.Millisecond
	default:
		return fmt.Errorf("tinysql: unsupported DSN option %q", key)
	}
	return nil
}

func parseDSNBool(value, key string) (bool, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "1", "true", "yes", "on":
		return true, nil
	case "0", "false", "no", "off":
		return false, nil
	default:
		return false, fmt.Errorf("tinysql: invalid %s boolean %q (use 0/1 or true/false)", key, value)
	}
}

func parseNonNegativeUint(value, key string) (uint64, error) {
	if strings.TrimSpace(value) == "" {
		return 0, fmt.Errorf("tinysql: %s must not be empty", key)
	}
	v, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("tinysql: invalid %s value %q", key, value)
	}
	return v, nil
}

func parseNonNegativeDuration(value, key string) (time.Duration, error) {
	if strings.TrimSpace(value) == "" {
		return 0, fmt.Errorf("tinysql: %s must not be empty", key)
	}
	d, err := time.ParseDuration(value)
	if err != nil || d < 0 {
		return 0, fmt.Errorf("tinysql: invalid %s duration %q", key, value)
	}
	return d, nil
}

// parseByteSize accepts a non-negative integer byte count or a binary/decimal
// suffix (KiB/MiB/GiB and KB/MB/GB). -1 is accepted only for options where it
// has an explicit documented meaning (checkpoint_max_bytes disables its size
// trigger). Values must fit an int64 so they can be handed directly to the
// storage layer without silent overflow.
func parseByteSize(value, key string, allowNegativeOne bool) (int64, error) {
	v := strings.TrimSpace(value)
	if allowNegativeOne && v == "-1" {
		return -1, nil
	}
	if v == "" || strings.HasPrefix(v, "-") {
		return 0, fmt.Errorf("tinysql: invalid %s size %q", key, value)
	}
	lower := strings.ToLower(v)
	multipliers := []struct {
		suffix string
		factor uint64
	}{
		{"kib", 1 << 10}, {"mib", 1 << 20}, {"gib", 1 << 30}, {"tib", 1 << 40},
		{"kb", 1000}, {"mb", 1000 * 1000}, {"gb", 1000 * 1000 * 1000}, {"tb", 1000 * 1000 * 1000 * 1000},
		{"b", 1},
	}
	factor := uint64(1)
	for _, unit := range multipliers {
		if strings.HasSuffix(lower, unit.suffix) {
			lower = strings.TrimSpace(strings.TrimSuffix(lower, unit.suffix))
			factor = unit.factor
			break
		}
	}
	if lower == "" {
		return 0, fmt.Errorf("tinysql: invalid %s size %q", key, value)
	}
	n, err := strconv.ParseUint(lower, 10, 64)
	if err != nil || n > uint64(^uint64(0))/factor || n*factor > uint64(^uint64(0)>>1) {
		return 0, fmt.Errorf("tinysql: invalid %s size %q", key, value)
	}
	return int64(n * factor), nil
}

func parsePoolSize(value, key string) (int, error) {
	if strings.TrimSpace(value) == "" {
		return 0, fmt.Errorf("tinysql: %s must not be empty", key)
	}
	n, err := strconv.Atoi(value)
	if err != nil {
		return 0, fmt.Errorf("tinysql: invalid %s value %q", key, value)
	}
	if n < 0 {
		return 0, fmt.Errorf("tinysql: %s must be >= 0", key)
	}
	return n, nil
}

func parseBusyTimeout(value string) (time.Duration, error) {
	if strings.TrimSpace(value) == "" {
		return 0, fmt.Errorf("tinysql: busy_timeout must not be empty")
	}
	isNumeric := true
	for _, r := range value {
		if r < '0' || r > '9' {
			isNumeric = false
			break
		}
	}
	if isNumeric {
		switch value {
		case "":
			return 0, nil
		default:
			sz, err := strconv.ParseInt(value, 10, 64)
			if err != nil {
				return 0, fmt.Errorf("tinysql: invalid busy_timeout value %q", value)
			}
			if sz < 0 {
				return 0, fmt.Errorf("tinysql: busy_timeout must be >= 0")
			}
			return time.Duration(sz) * time.Millisecond, nil
		}
	}
	dur, err := time.ParseDuration(value)
	if err != nil {
		return 0, fmt.Errorf("tinysql: invalid busy_timeout value %q", value)
	}
	if dur < 0 {
		return 0, fmt.Errorf("tinysql: busy_timeout must be >= 0")
	}
	return dur, nil
}
