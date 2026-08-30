package engine

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// ProcedureContext is passed to registered in-memory stored procedures.
// It lets procedures run SQL against the same DB/tenant without re-entering
// Execute's content lock.
type ProcedureContext struct {
	env      ExecEnv
	readOnly bool
}

// Context returns the statement context passed to Execute.
func (pc ProcedureContext) Context() context.Context {
	return pc.env.ctx
}

// Tenant returns the tenant/schema namespace used by the CALL statement.
func (pc ProcedureContext) Tenant() string {
	return pc.env.tenant
}

// ExecuteSQL parses and executes sql inside the current CALL. It reuses the
// current statement lock, so procedures can compose ordinary tinySQL
// statements without deadlocking.
func (pc ProcedureContext) ExecuteSQL(sql string) (*ResultSet, error) {
	stmt, err := NewParser(sql).ParseStatement()
	if err != nil {
		return nil, err
	}
	return pc.Execute(stmt)
}

// ExecuteSQLArgs safely substitutes positional ? parameters and executes the
// resulting statement inside the current CALL. Placeholders inside quoted SQL
// strings are left untouched. Values are rendered as SQL literals with string
// escaping and BLOB/JSON support, so handlers do not need fmt.Sprintf-based
// SQL construction.
func (pc ProcedureContext) ExecuteSQLArgs(sql string, args ...any) (*ResultSet, error) {
	bound, err := bindProcedureSQL(sql, args)
	if err != nil {
		return nil, err
	}
	return pc.ExecuteSQL(bound)
}

// Execute executes a parsed statement inside the current CALL.
func (pc ProcedureContext) Execute(stmt Statement) (*ResultSet, error) {
	if pc.readOnly && !isReadOnlyStatement(stmt) {
		return nil, fmt.Errorf("read-only stored procedure cannot execute mutating statement %T", stmt)
	}
	if err := checkPermission(pc.env.ctx, pc.env.db, stmt); err != nil {
		return nil, err
	}
	return execStmt(pc.env, stmt)
}

// StoredProcedureFunc is the Go handler signature for in-memory stored
// procedures registered with RegisterStoredProcedure.
type StoredProcedureFunc func(ctx ProcedureContext, args []any) (*ResultSet, error)

// StoredProcedureParameter describes one positional CALL argument. Required
// parameters must precede optional parameters. A nil Parameters slice on
// StoredProcedureOptions keeps legacy unchecked arity; a non-nil empty slice
// declares an exact zero-argument procedure.
type StoredProcedureParameter struct {
	Name        string
	Description string
	Required    bool
}

// StoredProcedureOptions controls validation, scheduling and rollback.
type StoredProcedureOptions struct {
	Description string
	Parameters  []StoredProcedureParameter
	// ReadOnly lets concurrent CALLs use the database content read lock. The
	// ProcedureContext enforces the promise for every nested statement.
	ReadOnly bool
	// Atomic takes a full statement rollback point for a mutating procedure,
	// restoring all nested table/catalog changes when its handler returns an
	// error. Read-only procedures never need this option.
	Atomic bool
}

// StoredProcedureInfo describes one registered procedure.
type StoredProcedureInfo struct {
	Name         string
	Description  string
	Parameters   []StoredProcedureParameter
	MinArgs      int
	MaxArgs      int
	ReadOnly     bool
	Atomic       bool
	RegisteredAt time.Time
	Calls        uint64
	Errors       uint64
	LastCalledAt time.Time
	TotalRuntime time.Duration
}

type storedProcedure struct {
	name         string
	fn           StoredProcedureFunc
	options      StoredProcedureOptions
	validateArgs bool
	minArgs      int
	maxArgs      int
	registeredAt time.Time
	stats        *storedProcedureStats
}

type storedProcedureStats struct {
	calls              atomic.Uint64
	errors             atomic.Uint64
	totalRuntimeNanos  atomic.Uint64
	lastCalledUnixNano atomic.Int64
}

var procedureRegistry = struct {
	sync.Mutex
	items map[string]storedProcedure
}{items: make(map[string]storedProcedure)}

var procedureRegistrySnapshot atomic.Pointer[map[string]storedProcedure]

// RegisterStoredProcedure registers an in-memory stored procedure by name.
// Names are case-insensitive. Re-registering the same name replaces the
// previous handler. Procedures are process-local and are not persisted.
func RegisterStoredProcedure(name string, fn StoredProcedureFunc) error {
	return RegisterStoredProcedureWithOptions(name, StoredProcedureOptions{}, fn)
}

// RegisterStoredProcedureWithOptions registers a procedure with argument,
// concurrency, rollback and catalog metadata. Names are case-insensitive;
// replacing a registration also resets its invocation statistics.
func RegisterStoredProcedureWithOptions(name string, options StoredProcedureOptions, fn StoredProcedureFunc) error {
	canonical := canonicalProcedureName(name)
	if canonical == "" {
		return fmt.Errorf("stored procedure name is required")
	}
	if fn == nil {
		return fmt.Errorf("stored procedure %q has nil handler", name)
	}
	normalized, validateArgs, minArgs, maxArgs, err := normalizeStoredProcedureOptions(options)
	if err != nil {
		return fmt.Errorf("stored procedure %q: %w", name, err)
	}
	if normalized.ReadOnly && normalized.Atomic {
		return fmt.Errorf("stored procedure %q: read-only and atomic options cannot be combined", name)
	}
	procedureRegistry.Lock()
	defer procedureRegistry.Unlock()
	procedureRegistry.items[canonical] = storedProcedure{
		name:         strings.TrimSpace(name),
		fn:           fn,
		options:      normalized,
		validateArgs: validateArgs,
		minArgs:      minArgs,
		maxArgs:      maxArgs,
		registeredAt: time.Now(),
		stats:        &storedProcedureStats{},
	}
	publishProcedureRegistryLocked()
	return nil
}

// UnregisterStoredProcedure removes a procedure. It returns true when a
// registered procedure existed.
func UnregisterStoredProcedure(name string) bool {
	canonical := canonicalProcedureName(name)
	if canonical == "" {
		return false
	}
	procedureRegistry.Lock()
	defer procedureRegistry.Unlock()
	if _, ok := procedureRegistry.items[canonical]; !ok {
		return false
	}
	delete(procedureRegistry.items, canonical)
	publishProcedureRegistryLocked()
	return true
}

// ListStoredProcedures returns registered in-memory procedures sorted by name.
func ListStoredProcedures() []StoredProcedureInfo {
	snapshot := procedureRegistrySnapshot.Load()
	if snapshot == nil {
		return nil
	}
	out := make([]StoredProcedureInfo, 0, len(*snapshot))
	for _, proc := range *snapshot {
		out = append(out, procedureInfo(proc))
	}
	sort.Slice(out, func(i, j int) bool {
		return strings.ToLower(out[i].Name) < strings.ToLower(out[j].Name)
	})
	return out
}

func lookupStoredProcedure(name string) (storedProcedure, bool) {
	canonical := canonicalProcedureName(name)
	if canonical == "" {
		return storedProcedure{}, false
	}
	snapshot := procedureRegistrySnapshot.Load()
	if snapshot == nil {
		return storedProcedure{}, false
	}
	proc, ok := (*snapshot)[canonical]
	return proc, ok
}

func publishProcedureRegistryLocked() {
	snapshot := make(map[string]storedProcedure, len(procedureRegistry.items))
	for name, procedure := range procedureRegistry.items {
		snapshot[name] = procedure
	}
	procedureRegistrySnapshot.Store(&snapshot)
}

func procedureInfo(proc storedProcedure) StoredProcedureInfo {
	info := StoredProcedureInfo{
		Name:         proc.name,
		Description:  proc.options.Description,
		Parameters:   append([]StoredProcedureParameter(nil), proc.options.Parameters...),
		MinArgs:      proc.minArgs,
		MaxArgs:      proc.maxArgs,
		ReadOnly:     proc.options.ReadOnly,
		Atomic:       proc.options.Atomic,
		RegisteredAt: proc.registeredAt,
	}
	if proc.stats != nil {
		info.Calls = proc.stats.calls.Load()
		info.Errors = proc.stats.errors.Load()
		info.TotalRuntime = time.Duration(proc.stats.totalRuntimeNanos.Load())
		if unixNano := proc.stats.lastCalledUnixNano.Load(); unixNano > 0 {
			info.LastCalledAt = time.Unix(0, unixNano)
		}
	}
	return info
}

func normalizeStoredProcedureOptions(options StoredProcedureOptions) (StoredProcedureOptions, bool, int, int, error) {
	options.Description = strings.TrimSpace(options.Description)
	if options.Parameters == nil {
		return options, false, 0, -1, nil
	}
	options.Parameters = append([]StoredProcedureParameter(nil), options.Parameters...)
	seen := make(map[string]struct{}, len(options.Parameters))
	optionalSeen := false
	minArgs := 0
	for i := range options.Parameters {
		parameter := &options.Parameters[i]
		parameter.Name = strings.TrimSpace(parameter.Name)
		parameter.Description = strings.TrimSpace(parameter.Description)
		if parameter.Name == "" {
			return StoredProcedureOptions{}, false, 0, 0, fmt.Errorf("parameter %d requires a name", i+1)
		}
		canonical := strings.ToLower(parameter.Name)
		if _, duplicate := seen[canonical]; duplicate {
			return StoredProcedureOptions{}, false, 0, 0, fmt.Errorf("duplicate parameter name %q", parameter.Name)
		}
		seen[canonical] = struct{}{}
		if parameter.Required {
			if optionalSeen {
				return StoredProcedureOptions{}, false, 0, 0, fmt.Errorf("required parameter %q follows an optional parameter", parameter.Name)
			}
			minArgs++
		} else {
			optionalSeen = true
		}
	}
	return options, true, minArgs, len(options.Parameters), nil
}

func canonicalProcedureName(name string) string {
	return strings.ToLower(strings.TrimSpace(name))
}

func executeCallProcedure(env ExecEnv, s *CallProcedure) (rs *ResultSet, err error) {
	proc, ok := lookupStoredProcedure(s.Name)
	if !ok {
		return nil, fmt.Errorf("unknown stored procedure: %s", s.Name)
	}
	if proc.validateArgs && (len(s.Args) < proc.minArgs || len(s.Args) > proc.maxArgs) {
		return nil, procedureArityError(proc, len(s.Args))
	}
	args := make([]any, len(s.Args))
	for i, arg := range s.Args {
		v, err := evalExpr(env, arg, Row{})
		if err != nil {
			return nil, fmt.Errorf("procedure %s argument %d: %w", s.Name, i+1, err)
		}
		args[i] = v
	}
	started := time.Now()
	if proc.stats != nil {
		proc.stats.calls.Add(1)
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			rs = nil
			err = fmt.Errorf("stored procedure %s panicked: %v", proc.name, recovered)
		}
		if proc.stats == nil {
			return
		}
		proc.stats.totalRuntimeNanos.Add(uint64(time.Since(started)))
		proc.stats.lastCalledUnixNano.Store(time.Now().UnixNano())
		if err != nil {
			proc.stats.errors.Add(1)
		}
	}()
	rs, err = proc.fn(ProcedureContext{env: env, readOnly: proc.options.ReadOnly}, args)
	if err == nil && rs == nil {
		rs = &ResultSet{}
	}
	return rs, err
}

func procedureArityError(proc storedProcedure, got int) error {
	switch {
	case proc.minArgs == proc.maxArgs:
		return fmt.Errorf("procedure %s expects %d arguments, got %d", proc.name, proc.minArgs, got)
	case proc.minArgs == 0:
		return fmt.Errorf("procedure %s expects at most %d arguments, got %d", proc.name, proc.maxArgs, got)
	default:
		return fmt.Errorf("procedure %s expects %d to %d arguments, got %d", proc.name, proc.minArgs, proc.maxArgs, got)
	}
}

func bindProcedureSQL(sql string, args []any) (string, error) {
	literals := make([]string, len(args))
	totalLiteralBytes := 0
	for i, arg := range args {
		literals[i] = procedureSQLLiteral(arg)
		totalLiteralBytes += len(literals[i])
	}
	var out strings.Builder
	out.Grow(len(sql) + totalLiteralBytes)
	argument := 0
	for i := 0; i < len(sql); i++ {
		ch := sql[i]
		if ch == '\'' {
			out.WriteByte(ch)
			i++
			for i < len(sql) {
				out.WriteByte(sql[i])
				if sql[i] == '\'' {
					if i+1 < len(sql) && sql[i+1] == '\'' {
						i++
						out.WriteByte(sql[i])
						i++
						continue
					}
					break
				}
				i++
			}
			continue
		}
		if ch != '?' {
			out.WriteByte(ch)
			continue
		}
		if argument >= len(literals) {
			return "", fmt.Errorf("stored procedure SQL has more placeholders than arguments")
		}
		out.WriteString(literals[argument])
		argument++
	}
	if argument != len(literals) {
		return "", fmt.Errorf("stored procedure SQL used %d of %d arguments", argument, len(literals))
	}
	return out.String(), nil
}

func procedureSQLLiteral(value any) string {
	switch v := value.(type) {
	case nil:
		return "NULL"
	case int:
		return strconv.Itoa(v)
	case int8:
		return strconv.FormatInt(int64(v), 10)
	case int16:
		return strconv.FormatInt(int64(v), 10)
	case int32:
		return strconv.FormatInt(int64(v), 10)
	case int64:
		return strconv.FormatInt(v, 10)
	case uint:
		return strconv.FormatUint(uint64(v), 10)
	case uint8:
		return strconv.FormatUint(uint64(v), 10)
	case uint16:
		return strconv.FormatUint(uint64(v), 10)
	case uint32:
		return strconv.FormatUint(uint64(v), 10)
	case uint64:
		return strconv.FormatUint(v, 10)
	case float32:
		return strconv.FormatFloat(float64(v), 'f', -1, 32)
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64)
	case bool:
		if v {
			return "TRUE"
		}
		return "FALSE"
	case string:
		return "'" + strings.ReplaceAll(v, "'", "''") + "'"
	case []byte:
		return "X'" + hex.EncodeToString(v) + "'"
	case time.Time:
		return "'" + v.Format(time.RFC3339Nano) + "'"
	default:
		encoded, err := json.Marshal(v)
		if err != nil {
			return "'" + strings.ReplaceAll(fmt.Sprint(v), "'", "''") + "'"
		}
		return "'" + strings.ReplaceAll(string(encoded), "'", "''") + "'"
	}
}
