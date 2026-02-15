package importer

import (
	"math/big"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// ============================================================================
// Type Inference - Detect column types from sample data
// ============================================================================

// inferColumnTypes analyzes sample data to determine the best tinySQL column type
// for each column. It tries in order: BOOL → INT → FLOAT → TIME → TEXT.
func inferColumnTypes(sampleData [][]string, numCols int, opts *ImportOptions) []storage.ColType {
	types := make([]storage.ColType, numCols)

	// Initialize type vote counters per column
	votes := make([]map[storage.ColType]int, numCols)
	for i := range votes {
		votes[i] = make(map[storage.ColType]int)
	}

	// Analyze each column across all sample rows
	for _, row := range sampleData {
		for colIdx := 0; colIdx < numCols; colIdx++ {
			var val string
			if colIdx < len(row) {
				val = strings.TrimSpace(row[colIdx])
			}

			// Skip null values in type inference
			if isNullValue(val, opts.NullLiterals) {
				continue
			}

			// Try to determine type in order of specificity
			detectedType := detectValueType(val, opts.DateTimeFormats)
			votes[colIdx][detectedType]++
		}
	}

	// Determine final type for each column based on votes
	for colIdx := 0; colIdx < numCols; colIdx++ {
		types[colIdx] = determineColumnType(votes[colIdx])
	}

	return types
}

// detectValueType attempts to parse a single value and returns its most specific type.
func detectValueType(val string, dateFormats []string) storage.ColType {
	if val == "" {
		return storage.TextType
	}

	// Boolean detection
	if isBoolLike(val) {
		return storage.BoolType
	}

	// Integer detection (including negative)
	if isIntLike(val) {
		return storage.IntType
	}

	// Float detection
	if isFloatLike(val) {
		return storage.Float64Type
	}

	// Time detection
	if isTimeLike(val, dateFormats) {
		return storage.TimeType
	}

	// Default to text
	return storage.TextType
}

// Helper: isBoolLike returns true for common boolean representations.
func isBoolLike(val string) bool {
	lower := strings.ToLower(strings.TrimSpace(val))
	switch lower {
	case "true", "false", "yes", "no":
		return true
	case "t", "f", "y", "n":
		return len(val) == 1
	default:
		return false
	}
}

// Helper: isIntLike returns true if the value parses as an integer.
func isIntLike(val string) bool {
	_, err := strconv.ParseInt(strings.TrimSpace(val), 10, 64)
	return err == nil
}

// Helper: isFloatLike returns true if the value parses as a float.
func isFloatLike(val string) bool {
	_, err := strconv.ParseFloat(strings.TrimSpace(val), 64)
	return err == nil
}

// Helper: isTimeLike attempts to parse the value against provided layouts.
func isTimeLike(val string, layouts []string) bool {
	for _, l := range layouts {
		if _, err := time.Parse(l, val); err == nil {
			return true
		}
	}
	return false
}

// determineColumnType picks the final type based on vote counts.
// Strategy: Pick most specific type that covers >80% of non-null values, else TEXT.
func determineColumnType(votes map[storage.ColType]int) storage.ColType {
	if len(votes) == 0 {
		return storage.TextType
	}

	totalVotes := 0
	for _, count := range votes {
		totalVotes += count
	}

	if totalVotes == 0 {
		return storage.TextType
	}

	// Type precedence: specific → general
	// BOOL and TIME are very specific, so require high confidence
	// INT is promoted to FLOAT if mixed
	// Everything else defaults to TEXT

	boolCount := votes[storage.BoolType]
	intCount := votes[storage.IntType]
	floatCount := votes[storage.Float64Type]
	timeCount := votes[storage.TimeType]
	// textCount := votes[storage.TextType] // Keep for potential future use

	threshold := float64(totalVotes) * 0.80

	// If 80%+ are boolean, use BOOL
	if float64(boolCount) >= threshold {
		return storage.BoolType
	}

	// If 80%+ are time, use TIME
	if float64(timeCount) >= threshold {
		return storage.TimeType
	}

	// If 80%+ are int, use INT (unless there are floats)
	if float64(intCount) >= threshold && floatCount == 0 {
		return storage.IntType
	}

	// If 80%+ are int or float combined, use FLOAT
	if float64(intCount+floatCount) >= threshold {
		return storage.Float64Type
	}

	// Default to TEXT
	return storage.TextType
}

// isNullValue checks if a value should be treated as NULL.
func isNullValue(val string, nullLiterals []string) bool {
	trimmed := strings.ToLower(strings.TrimSpace(val))
	for _, nl := range nullLiterals {
		if trimmed == strings.ToLower(strings.TrimSpace(nl)) {
			return true
		}
	}
	return false
}

// convertValue converts a string value to the appropriate Go type based on the column type.
func convertValue(val string, colType storage.ColType, dateFormats []string, nullLiterals []string) (any, error) {
	val = strings.TrimSpace(val)

	// Handle NULL values
	if isNullValue(val, nullLiterals) {
		return nil, nil
	}

	switch colType {
	case storage.BoolType:
		return parseBool(val)

	case storage.IntType, storage.Int8Type, storage.Int16Type, storage.Int32Type, storage.Int64Type:
		return strconv.ParseInt(val, 10, 64)

	case storage.UintType, storage.Uint8Type, storage.Uint16Type, storage.Uint32Type, storage.Uint64Type:
		return strconv.ParseUint(val, 10, 64)

	case storage.Float32Type, storage.Float64Type, storage.FloatType:
		return strconv.ParseFloat(val, 64)

	case storage.TimeType, storage.DateType, storage.DateTimeType, storage.TimestampType:
		return parseDateTime(val, dateFormats)

	case storage.DecimalType, storage.MoneyType:
		// Use big.Rat for arbitrary precision decimal representation
		r := new(big.Rat)
		if _, ok := r.SetString(val); !ok {
			return nil, strconv.ErrSyntax
		}
		return r, nil

	case storage.UUIDType:
		u, err := uuid.Parse(val)
		if err != nil {
			return nil, err
		}
		return u, nil

	case storage.BlobType:
		// For CSV/JSON import, treat as raw bytes of the string value
		return []byte(val), nil

	case storage.XMLType:
		// Keep raw XML as string
		return val, nil

	case storage.IntervalType:
		// Parse as Go duration where possible
		if d, err := time.ParseDuration(val); err == nil {
			return d, nil
		}
		// Fallback to string
		return val, nil

	default:
		// TEXT and all other types
		return val, nil
	}
}

// parseBool handles various boolean representations.
func parseBool(val string) (bool, error) {
	lower := strings.ToLower(strings.TrimSpace(val))
	switch lower {
	case "true", "t", "yes", "y", "1":
		return true, nil
	case "false", "f", "no", "n", "0":
		return false, nil
	default:
		return strconv.ParseBool(val)
	}
}

// parseDateTime tries multiple datetime formats.
func parseDateTime(val string, formats []string) (time.Time, error) {
	for _, layout := range formats {
		if t, err := time.Parse(layout, val); err == nil {
			return t, nil
		}
	}
	return time.Time{}, strconv.ErrSyntax
}
