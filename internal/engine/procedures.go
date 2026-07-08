package engine

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

// ProcedureContext is passed to registered in-memory stored procedures.
// It lets procedures run SQL against the same DB/tenant without re-entering
// Execute's content lock.
type ProcedureContext struct {
	env ExecEnv
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

// Execute executes a parsed statement inside the current CALL.
func (pc ProcedureContext) Execute(stmt Statement) (*ResultSet, error) {
	if err := checkPermission(pc.env.ctx, pc.env.db, stmt); err != nil {
		return nil, err
	}
	return execStmt(pc.env, stmt)
}

// StoredProcedureFunc is the Go handler signature for in-memory stored
// procedures registered with RegisterStoredProcedure.
type StoredProcedureFunc func(ctx ProcedureContext, args []any) (*ResultSet, error)

// StoredProcedureInfo describes one registered procedure.
type StoredProcedureInfo struct {
	Name         string
	RegisteredAt time.Time
}

type storedProcedure struct {
	name         string
	fn           StoredProcedureFunc
	registeredAt time.Time
}

var procedureRegistry = struct {
	sync.RWMutex
	items map[string]storedProcedure
}{items: make(map[string]storedProcedure)}

// RegisterStoredProcedure registers an in-memory stored procedure by name.
// Names are case-insensitive. Re-registering the same name replaces the
// previous handler. Procedures are process-local and are not persisted.
func RegisterStoredProcedure(name string, fn StoredProcedureFunc) error {
	canonical := canonicalProcedureName(name)
	if canonical == "" {
		return fmt.Errorf("stored procedure name is required")
	}
	if fn == nil {
		return fmt.Errorf("stored procedure %q has nil handler", name)
	}
	procedureRegistry.Lock()
	defer procedureRegistry.Unlock()
	procedureRegistry.items[canonical] = storedProcedure{
		name:         strings.TrimSpace(name),
		fn:           fn,
		registeredAt: time.Now(),
	}
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
	return true
}

// ListStoredProcedures returns registered in-memory procedures sorted by name.
func ListStoredProcedures() []StoredProcedureInfo {
	procedureRegistry.RLock()
	defer procedureRegistry.RUnlock()
	out := make([]StoredProcedureInfo, 0, len(procedureRegistry.items))
	for _, proc := range procedureRegistry.items {
		out = append(out, StoredProcedureInfo{Name: proc.name, RegisteredAt: proc.registeredAt})
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
	procedureRegistry.RLock()
	defer procedureRegistry.RUnlock()
	proc, ok := procedureRegistry.items[canonical]
	return proc, ok
}

func canonicalProcedureName(name string) string {
	return strings.ToLower(strings.TrimSpace(name))
}

func executeCallProcedure(env ExecEnv, s *CallProcedure) (*ResultSet, error) {
	proc, ok := lookupStoredProcedure(s.Name)
	if !ok {
		return nil, fmt.Errorf("unknown stored procedure: %s", s.Name)
	}
	args := make([]any, len(s.Args))
	for i, arg := range s.Args {
		v, err := evalExpr(env, arg, Row{})
		if err != nil {
			return nil, fmt.Errorf("procedure %s argument %d: %w", s.Name, i+1, err)
		}
		args[i] = v
	}
	return proc.fn(ProcedureContext{env: env}, args)
}
