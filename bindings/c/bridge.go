//go:build cgo

// Package main implements tinySQL's handle-based C ABI. One static archive or
// shared library serves the Swift package, the Python and Rust bindings and
// any other C-compatible host; include/tinysql.h documents the contract.
package main

import (
	"bufio"
	"compress/gzip"
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"strings"
	"sync"
	"time"

	tsql "github.com/SimonWaldherr/tinySQL"
	tsqldriver "github.com/SimonWaldherr/tinySQL/driver"
	"github.com/SimonWaldherr/tinySQL/internal/sqlbind"
	"github.com/SimonWaldherr/tinySQL/sqlutil"
)

// abiVersion increases whenever functions are added to include/tinysql.h.
// Existing functions keep their signatures and response shapes.
const abiVersion = 2

type database struct {
	mu     sync.Mutex
	db     *tsql.DB
	pool   *sql.DB
	conn   *sql.Conn // Pin the connection so SQL transactions span calls.
	closed bool
}

// response is encoded by appendResponse. Rows hold driver values (int64,
// float64, bool, string, []byte, time.Time or nil); the encoder applies the
// ABI's tagging for doubles and BLOBs.
type response struct {
	Error        string
	Handle       uint64
	Columns      []string
	Rows         [][]any
	RowsAffected int64
	Statements   int
	// InTransaction is reported after statements: true while the handle's
	// pinned connection is inside BEGIN ... COMMIT/ROLLBACK.
	InTransaction bool
	Version       string
	ABI           int
}

var databases = struct {
	sync.Mutex
	next    uint64
	entries map[uint64]*database
}{entries: make(map[uint64]*database)}

// openOptions is the JSON accepted by TinySQLDatabaseOpenWithOptions.
type openOptions struct {
	Path                 string `json:"path"`
	Mode                 string `json:"mode"`
	ReadOnly             bool   `json:"readOnly"`
	MaxMemoryBytes       int64  `json:"maxMemoryBytes"`
	SyncOnMutate         bool   `json:"syncOnMutate"`
	CompressFiles        bool   `json:"compressFiles"`
	CheckpointEvery      uint64 `json:"checkpointEvery"`
	CheckpointIntervalMs int64  `json:"checkpointIntervalMs"`
	CheckpointMaxBytes   int64  `json:"checkpointMaxBytes"`
	WALSync              string `json:"walSync"`
	EncryptionKey        string `json:"encryptionKey"` // base64, 32 bytes
}

func openDatabase(path string) (response, error) {
	if path == "" {
		return register(tsql.NewDB())
	}
	db, err := loadSnapshot(path)
	if err != nil {
		return response{}, err
	}
	return register(db)
}

func openWithOptions(input string) (response, error) {
	var options openOptions
	if strings.TrimSpace(input) != "" {
		dec := json.NewDecoder(strings.NewReader(input))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&options); err != nil {
			return response{}, fmt.Errorf("options: %w", err)
		}
		if dec.More() {
			return response{}, errors.New("options must contain one JSON object")
		}
	}
	db, err := openStorage(options)
	if err != nil {
		return response{}, err
	}
	return register(db)
}

func openStorage(o openOptions) (*tsql.DB, error) {
	mode := strings.ToLower(strings.TrimSpace(o.Mode))
	if mode == "" {
		if o.Path == "" {
			mode = "memory"
		} else {
			mode = "snapshot"
		}
	}
	if mode == "snapshot" {
		if o.Path == "" {
			return nil, errors.New("snapshot mode requires a path")
		}
		db, err := loadSnapshot(o.Path)
		if err != nil {
			return nil, err
		}
		if o.ReadOnly {
			db.SetReadOnly(true)
		}
		return db, nil
	}
	storageMode, err := tsql.ParseStorageMode(mode)
	if err != nil {
		return nil, err
	}
	if storageMode == tsql.ModeMemory {
		if o.Path != "" {
			return nil, errors.New("memory mode does not take a path; use mode \"snapshot\" to load one")
		}
		db := tsql.NewDB()
		if o.ReadOnly {
			db.SetReadOnly(true)
		}
		return db, nil
	}
	if o.Path == "" {
		return nil, fmt.Errorf("mode %q requires a path", mode)
	}
	cfg := tsql.DefaultStorageConfig(storageMode)
	cfg.Path = o.Path
	cfg.ReadOnly = o.ReadOnly
	cfg.SyncOnMutate = o.SyncOnMutate
	cfg.CompressFiles = o.CompressFiles
	if o.MaxMemoryBytes > 0 {
		cfg.MaxMemoryBytes = o.MaxMemoryBytes
	}
	if o.CheckpointEvery > 0 {
		cfg.CheckpointEvery = o.CheckpointEvery
	}
	if o.CheckpointIntervalMs > 0 {
		cfg.CheckpointInterval = time.Duration(o.CheckpointIntervalMs) * time.Millisecond
	}
	if o.CheckpointMaxBytes != 0 {
		cfg.CheckpointMaxBytes = o.CheckpointMaxBytes
	}
	if o.WALSync != "" {
		if cfg.WALSync, err = tsql.ParseWALSyncMode(o.WALSync); err != nil {
			return nil, err
		}
	}
	if o.EncryptionKey != "" {
		if cfg.EncryptionKey, err = base64.StdEncoding.DecodeString(o.EncryptionKey); err != nil {
			return nil, fmt.Errorf("encryptionKey: %w", err)
		}
	}
	return tsql.OpenDB(cfg)
}

// register binds a pinned database/sql connection to db and returns a new
// handle. It takes ownership of db, including on error.
func register(db *tsql.DB) (response, error) {
	pool, err := tsqldriver.OpenWithDB(db)
	if err != nil {
		_ = db.Close()
		return response{}, err
	}
	pool.SetMaxOpenConns(1)
	conn, err := pool.Conn(context.Background())
	if err != nil {
		_ = pool.Close()
		_ = db.Close()
		return response{}, err
	}
	databases.Lock()
	defer databases.Unlock()
	// Never reuse an ID, including after overflow, so stale handles stay invalid.
	if databases.next == ^uint64(0) {
		_ = conn.Close()
		_ = pool.Close()
		_ = db.Close()
		return response{}, errors.New("database handle space exhausted")
	}
	databases.next++
	id := databases.next
	databases.entries[id] = &database{db: db, pool: pool, conn: conn}
	return response{Handle: id}, nil
}

func loadSnapshot(path string) (*tsql.DB, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, errors.New("snapshot must be a regular file")
	}
	var reader io.Reader = file
	if strings.HasSuffix(strings.ToLower(path), ".gz") {
		compressed, err := gzip.NewReader(file)
		if err != nil {
			return nil, err
		}
		defer compressed.Close()
		reader = compressed
	}
	buffered := bufio.NewReader(reader)
	if _, err := buffered.Peek(1); err != nil {
		return nil, fmt.Errorf("empty or unreadable snapshot: %w", err)
	}
	// Loading through a reader deliberately avoids LoadFromFile's implicit
	// WAL attachment, which would mutate snapshots (including app resources).
	db, err := tsql.LoadFromReader(buffered)
	if err != nil {
		return nil, err
	}
	// The snapshot decoder may stop before the gzip trailer. Drain the reader
	// to validate the checksum and detect truncated compressed snapshots.
	if _, err := io.Copy(io.Discard, buffered); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("snapshot integrity: %w", err)
	}
	return db, nil
}

func withDatabase(id uint64, fn func(*database) (response, error)) (response, error) {
	databases.Lock()
	db := databases.entries[id]
	databases.Unlock()
	if db == nil {
		return response{}, errors.New("invalid or closed database handle")
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return response{}, errors.New("database is closed")
	}
	return fn(db)
}

func closeDatabase(id uint64) (response, error) {
	return withDatabase(id, func(d *database) (response, error) {
		d.closed = true
		databases.Lock()
		delete(databases.entries, id)
		databases.Unlock()
		// Closing the pinned connection releases any pending transaction
		// resources; closing the database flushes durable storage modes.
		return response{}, errors.Join(d.conn.Close(), d.pool.Close(), d.db.Close())
	})
}

func decodeParameters(input string) ([]any, error) {
	if input == "" {
		return nil, nil
	}
	dec := json.NewDecoder(strings.NewReader(input))
	dec.UseNumber()
	var values []any
	if err := dec.Decode(&values); err != nil {
		return nil, fmt.Errorf("parameters: %w", err)
	}
	if values == nil {
		return nil, errors.New("parameters must be a JSON array")
	}
	var trailing any
	if err := dec.Decode(&trailing); err != io.EOF {
		return nil, errors.New("parameters must contain one JSON array")
	}
	return convertParameters(values, "parameter")
}

// decodeBatch accepts a JSON array of parameter arrays.
func decodeBatch(input string) ([][]any, error) {
	dec := json.NewDecoder(strings.NewReader(input))
	dec.UseNumber()
	var sets [][]any
	if err := dec.Decode(&sets); err != nil {
		return nil, fmt.Errorf("parameters: %w", err)
	}
	if sets == nil {
		return nil, errors.New("batch parameters must be a JSON array of arrays")
	}
	var trailing any
	if err := dec.Decode(&trailing); err != io.EOF {
		return nil, errors.New("batch parameters must contain one JSON array")
	}
	for i, set := range sets {
		if set == nil {
			return nil, fmt.Errorf("row %d: parameters must be a JSON array", i)
		}
		converted, err := convertParameters(set, fmt.Sprintf("row %d parameter", i))
		if err != nil {
			return nil, err
		}
		sets[i] = converted
	}
	return sets, nil
}

func convertParameters(values []any, label string) ([]any, error) {
	for i, value := range values {
		switch v := value.(type) {
		case nil, string, bool:
		case json.Number:
			if !strings.ContainsAny(string(v), ".eE") {
				n, err := v.Int64()
				if err != nil {
					return nil, fmt.Errorf("%s %d: %w", label, i, err)
				}
				values[i] = n
			} else {
				n, err := v.Float64()
				if err != nil {
					return nil, fmt.Errorf("%s %d: %w", label, i, err)
				}
				values[i] = n
			}
		case map[string]any:
			if real, ok := v["real"].(json.Number); ok && len(v) == 1 {
				n, err := real.Float64()
				if err != nil {
					return nil, fmt.Errorf("%s %d: %w", label, i, err)
				}
				values[i] = n
				continue
			}
			encoded, ok := v["blob"].(string)
			if !ok || len(v) != 1 {
				return nil, fmt.Errorf("%s %d: expected a real or base64 blob object", label, i)
			}
			blob, err := base64.StdEncoding.DecodeString(encoded)
			if err != nil {
				return nil, fmt.Errorf("%s %d: %w", label, i, err)
			}
			values[i] = blob
		default:
			return nil, fmt.Errorf("%s %d: unsupported value", label, i)
		}
	}
	return values, nil
}

// statementMode selects the database/sql entry point for one call.
type statementMode int

const (
	modeExec statementMode = iota
	modeQuery
	modeAuto
)

func runSQL(id uint64, query, parameters string, mode statementMode) (response, error) {
	args, err := decodeParameters(parameters)
	if err != nil {
		return response{}, err
	}
	if mode == modeAuto {
		mode = modeExec
		if returnsRows(query) {
			mode = modeQuery
		}
	}
	return withStatement(id, func(d *database) (response, error) {
		if mode == modeExec {
			result, err := d.conn.ExecContext(context.Background(), query, args...)
			if err != nil {
				return response{}, err
			}
			count, err := result.RowsAffected()
			return response{RowsAffected: count}, err
		}
		return queryRows(d.conn, query, args)
	})
}

// withStatement runs fn like withDatabase and reports the transaction state
// afterwards, including when fn fails part-way through a script or batch.
func withStatement(id uint64, fn func(*database) (response, error)) (response, error) {
	return withDatabase(id, func(d *database) (response, error) {
		r, err := fn(d)
		return d.withTransactionState(r), err
	})
}

// withTransactionState records whether the pinned connection is inside a
// transaction, so hosts can mirror SQL-level BEGIN/COMMIT/ROLLBACK.
func (d *database) withTransactionState(r response) response {
	_ = d.conn.Raw(func(driverConn any) error {
		if state, ok := driverConn.(interface{ InTransaction() bool }); ok {
			r.InTransaction = state.InTransaction()
		}
		return nil
	})
	return r
}

func queryRows(conn *sql.Conn, query string, args []any) (response, error) {
	rows, err := conn.QueryContext(context.Background(), query, args...)
	if err != nil {
		return response{}, err
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		return response{}, err
	}
	result := response{Columns: columns}
	// One backing array holds every cell; rows are windows into it. This
	// replaces a per-row slice allocation plus a per-row copy.
	width := len(columns)
	destinations := make([]any, width)
	var cells []any
	for rows.Next() {
		start := len(cells)
		for range width {
			cells = append(cells, nil)
		}
		for i := range destinations {
			destinations[i] = &cells[start+i]
		}
		if err := rows.Scan(destinations...); err != nil {
			return response{}, err
		}
	}
	if err := rows.Err(); err != nil {
		return response{}, err
	}
	if width > 0 && len(cells) > 0 {
		result.Rows = make([][]any, len(cells)/width)
		for i := range result.Rows {
			result.Rows[i] = cells[i*width : (i+1)*width : (i+1)*width]
		}
	}
	return result, rows.Close()
}

// executeBatch prepares sql once and executes it for every parameter set.
func executeBatch(id uint64, query, parameters string) (response, error) {
	sets, err := decodeBatch(parameters)
	if err != nil {
		return response{}, err
	}
	return withStatement(id, func(d *database) (response, error) {
		ctx := context.Background()
		stmt, err := d.conn.PrepareContext(ctx, query)
		if err != nil {
			return response{}, err
		}
		defer stmt.Close()
		var total int64
		for i, args := range sets {
			result, err := stmt.ExecContext(ctx, args...)
			if err != nil {
				return response{}, fmt.Errorf("row %d: %w", i, err)
			}
			if count, err := result.RowsAffected(); err == nil && count > 0 {
				total += count
			}
		}
		return response{RowsAffected: total, Statements: len(sets)}, nil
	})
}

// executeScript runs every statement of script on the pinned connection, so
// BEGIN/COMMIT inside the script behave as they do across separate calls.
func executeScript(id uint64, script string) (response, error) {
	statements := sqlutil.SplitStatements(script)
	return withStatement(id, func(d *database) (response, error) {
		var total int64
		for i, statement := range statements {
			result, err := d.conn.ExecContext(context.Background(), statement)
			if err != nil {
				return response{}, fmt.Errorf("statement %d: %w", i+1, err)
			}
			if count, err := result.RowsAffected(); err == nil && count > 0 {
				total += count
			}
		}
		return response{RowsAffected: total, Statements: len(statements)}, nil
	})
}

// returnsRows decides whether a statement is run through QueryContext. The
// query path also executes statements without rows correctly; classifying
// a write as a query only loses its affected-row count.
func returnsRows(query string) bool {
	word, rest := firstWord(query)
	switch word {
	case "INSERT", "UPDATE", "DELETE", "REPLACE", "UPSERT":
		return containsWord(rest, "RETURNING")
	case "CREATE", "DROP", "ALTER", "BEGIN", "COMMIT", "ROLLBACK", "END", "START",
		"SAVEPOINT", "RELEASE", "REFRESH", "GRANT", "REVOKE", "TRUNCATE", "SET":
		return false
	default:
		// SELECT, WITH, VALUES, EXPLAIN, PRAGMA, CALL, ANALYZE and bare table
		// names all produce rows.
		return true
	}
}

// firstWord returns the upper-cased first keyword after comments,
// whitespace and opening parentheses, plus the remaining text.
func firstWord(query string) (string, string) {
	i := 0
	for i < len(query) {
		if end := sqlbind.SkipOpaque(query, i); end > i && query[i] != '\'' && query[i] != '"' && query[i] != '`' {
			i = end
			continue
		}
		switch query[i] {
		case ' ', '\t', '\n', '\r', '\f', '\v', '(':
			i++
			continue
		}
		break
	}
	start := i
	for i < len(query) && isWordByte(query[i]) {
		i++
	}
	return strings.ToUpper(query[start:i]), query[i:]
}

func containsWord(text, word string) bool {
	for i := 0; i < len(text); {
		if end := sqlbind.SkipOpaque(text, i); end > i {
			i = end
			continue
		}
		if !isWordByte(text[i]) {
			i++
			continue
		}
		start := i
		for i < len(text) && isWordByte(text[i]) {
			i++
		}
		if strings.EqualFold(text[start:i], word) {
			return true
		}
	}
	return false
}

func isWordByte(b byte) bool {
	return b == '_' || b >= '0' && b <= '9' || b >= 'a' && b <= 'z' || b >= 'A' && b <= 'Z' || b >= 0x80
}

func saveDatabase(id uint64, path string) (response, error) {
	if path == "" {
		return response{}, errors.New("snapshot path must not be empty")
	}
	return withDatabase(id, func(d *database) (response, error) {
		return response{}, tsql.SaveToFile(d.db, path)
	})
}

func syncDatabase(id uint64) (response, error) {
	return withDatabase(id, func(d *database) (response, error) {
		return response{}, d.db.Sync()
	})
}

func info() (response, error) {
	return response{Version: tsql.Version(), ABI: abiVersion}, nil
}

// A failed JSON encoding must be an error response, never an empty C string.
func encodeResponse(fn func() (response, error)) (encoded []byte) {
	defer func() {
		if p := recover(); p != nil {
			encoded = appendError(nil, fmt.Sprintf("tinySQL panic: %v", p))
		}
	}()
	value, err := fn()
	if err != nil {
		// An error response carries no result, only the transaction state.
		return appendResponseError(nil, err.Error(), value.InTransaction)
	}
	encoded, err = appendResponse(make([]byte, 0, 64), value)
	if err != nil {
		return appendError(nil, "encode result: "+err.Error())
	}
	return encoded
}

func appendError(dst []byte, message string) []byte {
	return appendResponseError(dst, message, false)
}

func appendResponseError(dst []byte, message string, inTransaction bool) []byte {
	if message == "" {
		message = "unknown error"
	}
	dst = append(dst, `{"error":`...)
	dst = appendJSONString(dst, message)
	if inTransaction {
		dst = append(dst, `,"inTransaction":true`...)
	}
	return append(dst, '}')
}

// appendResponse writes the response object. Zero fields are omitted, which
// matches the shapes documented in include/tinysql.h.
func appendResponse(dst []byte, r response) ([]byte, error) {
	if r.Error != "" {
		return appendResponseError(dst, r.Error, r.InTransaction), nil
	}
	dst = append(dst, '{')
	first := true
	field := func(name string) {
		if !first {
			dst = append(dst, ',')
		}
		first = false
		dst = append(dst, '"')
		dst = append(dst, name...)
		dst = append(dst, '"', ':')
	}
	if r.Handle != 0 {
		field("handle")
		dst = appendUint(dst, r.Handle)
	}
	if len(r.Columns) > 0 {
		field("columns")
		dst = append(dst, '[')
		for i, column := range r.Columns {
			if i > 0 {
				dst = append(dst, ',')
			}
			dst = appendJSONString(dst, column)
		}
		dst = append(dst, ']')
	}
	if len(r.Rows) > 0 {
		field("rows")
		dst = append(dst, '[')
		var err error
		for i, row := range r.Rows {
			if i > 0 {
				dst = append(dst, ',')
			}
			dst = append(dst, '[')
			for j, value := range row {
				if j > 0 {
					dst = append(dst, ',')
				}
				if dst, err = appendValue(dst, value); err != nil {
					return nil, err
				}
			}
			dst = append(dst, ']')
		}
		dst = append(dst, ']')
	}
	if r.RowsAffected != 0 {
		field("rowsAffected")
		dst = appendInt(dst, r.RowsAffected)
	}
	if r.Statements != 0 {
		field("statements")
		dst = appendInt(dst, int64(r.Statements))
	}
	if r.InTransaction {
		field("inTransaction")
		dst = append(dst, "true"...)
	}
	if r.Version != "" {
		field("version")
		dst = appendJSONString(dst, r.Version)
	}
	if r.ABI != 0 {
		field("abi")
		dst = appendInt(dst, int64(r.ABI))
	}
	return append(dst, '}'), nil
}

// appendValue encodes one result cell. Doubles are tagged as {"real":n} so
// integral values keep their type; BLOBs are {"blob":"base64"}.
func appendValue(dst []byte, value any) ([]byte, error) {
	switch v := value.(type) {
	case nil:
		return append(dst, "null"...), nil
	case int64:
		return appendInt(dst, v), nil
	case int:
		return appendInt(dst, int64(v)), nil
	case float64:
		if math.IsNaN(v) || math.IsInf(v, 0) {
			return nil, fmt.Errorf("unsupported non-finite REAL value %v", v)
		}
		dst = append(dst, `{"real":`...)
		dst = appendFloat(dst, v)
		return append(dst, '}'), nil
	case bool:
		if v {
			return append(dst, "true"...), nil
		}
		return append(dst, "false"...), nil
	case string:
		return appendJSONString(dst, v), nil
	case []byte:
		dst = append(dst, `{"blob":"`...)
		dst = base64.StdEncoding.AppendEncode(dst, v)
		return append(dst, `"}`...), nil
	case time.Time:
		return appendJSONString(dst, v.Format(time.RFC3339Nano)), nil
	default:
		encoded, err := json.Marshal(v)
		if err != nil {
			return nil, err
		}
		return append(dst, encoded...), nil
	}
}
