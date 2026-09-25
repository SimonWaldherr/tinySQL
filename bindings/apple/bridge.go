//go:build cgo

// Package main implements the handle-based C ABI used by the Swift package.
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
	"os"
	"strings"
	"sync"

	tsql "github.com/SimonWaldherr/tinySQL"
	tsqldriver "github.com/SimonWaldherr/tinySQL/driver"
)

type database struct {
	mu     sync.Mutex
	db     *tsql.DB
	pool   *sql.DB
	conn   *sql.Conn // Pin the connection so SQL transactions span calls.
	closed bool
}

type response struct {
	Error        string   `json:"error,omitempty"`
	Handle       uint64   `json:"handle,omitempty"`
	Columns      []string `json:"columns,omitempty"`
	Rows         [][]any  `json:"rows,omitempty"`
	RowsAffected int64    `json:"rowsAffected,omitempty"`
}

var databases = struct {
	sync.Mutex
	next    uint64
	entries map[uint64]*database
}{entries: make(map[uint64]*database)}

func openDatabase(path string) (response, error) {
	var db *tsql.DB
	var err error
	if path == "" {
		db = tsql.NewDB()
	} else {
		db, err = loadSnapshot(path)
	}
	if err != nil {
		return response{}, err
	}
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
		// Closing the pinned connection releases any pending transaction resources.
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
	for i, value := range values {
		switch v := value.(type) {
		case nil, string, bool:
		case json.Number:
			if !strings.ContainsAny(string(v), ".eE") {
				n, err := v.Int64()
				if err != nil {
					return nil, fmt.Errorf("parameter %d: %w", i, err)
				}
				values[i] = n
			} else {
				n, err := v.Float64()
				if err != nil {
					return nil, fmt.Errorf("parameter %d: %w", i, err)
				}
				values[i] = n
			}
		case map[string]any:
			if real, ok := v["real"].(json.Number); ok && len(v) == 1 {
				n, err := real.Float64()
				if err != nil {
					return nil, fmt.Errorf("parameter %d: %w", i, err)
				}
				values[i] = n
				continue
			}
			encoded, ok := v["blob"].(string)
			if !ok || len(v) != 1 {
				return nil, fmt.Errorf("parameter %d: expected a real or base64 blob object", i)
			}
			blob, err := base64.StdEncoding.DecodeString(encoded)
			if err != nil {
				return nil, fmt.Errorf("parameter %d: %w", i, err)
			}
			values[i] = blob
		default:
			return nil, fmt.Errorf("parameter %d: unsupported value", i)
		}
	}
	return values, nil
}

func runSQL(id uint64, query, parameters string, queryRows bool) (response, error) {
	args, err := decodeParameters(parameters)
	if err != nil {
		return response{}, err
	}
	return withDatabase(id, func(d *database) (response, error) {
		if !queryRows {
			result, err := d.conn.ExecContext(context.Background(), query, args...)
			if err != nil {
				return response{}, err
			}
			count, err := result.RowsAffected()
			return response{RowsAffected: count}, err
		}
		rows, err := d.conn.QueryContext(context.Background(), query, args...)
		if err != nil {
			return response{}, err
		}
		defer rows.Close()
		columns, err := rows.Columns()
		if err != nil {
			return response{}, err
		}
		result := response{Columns: columns, Rows: make([][]any, 0)}
		// Reuse scan destinations. Each row still owns its values, but scanning
		// no longer allocates a second interface slice for every result row.
		values := make([]any, len(columns))
		destinations := make([]any, len(columns))
		for i := range values {
			destinations[i] = &values[i]
		}
		for rows.Next() {
			if err := rows.Scan(destinations...); err != nil {
				return response{}, err
			}
			for i, value := range values {
				switch v := value.(type) {
				case []byte:
					values[i] = map[string]string{"blob": base64.StdEncoding.EncodeToString(v)}
				case float64:
					values[i] = map[string]float64{"real": v}
				}
			}
			result.Rows = append(result.Rows, append([]any(nil), values...))
		}
		if err := rows.Err(); err != nil {
			return response{}, err
		}
		return result, rows.Close()
	})
}

func saveDatabase(id uint64, path string) (response, error) {
	if path == "" {
		return response{}, errors.New("snapshot path must not be empty")
	}
	return withDatabase(id, func(d *database) (response, error) {
		return response{}, tsql.SaveToFile(d.db, path)
	})
}

// A failed JSON encoding must be an error response, never an empty C string.
func encodeResponse(fn func() (response, error)) (encoded []byte) {
	defer func() {
		if p := recover(); p != nil {
			encoded, _ = json.Marshal(response{Error: fmt.Sprintf("tinySQL panic: %v", p)})
		}
	}()
	value, err := fn()
	if err != nil {
		value = response{Error: err.Error()}
	}
	encoded, err = json.Marshal(value)
	if err != nil {
		encoded, _ = json.Marshal(response{Error: "encode result: " + err.Error()})
	}
	return encoded
}
