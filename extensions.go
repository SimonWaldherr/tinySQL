package tinysql

import (
	"context"

	"github.com/SimonWaldherr/tinySQL/internal/engine"
	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// ProcedureContext is passed to in-memory stored procedures registered with
// RegisterStoredProcedure. It can execute nested SQL within the same CALL.
type ProcedureContext = engine.ProcedureContext

// StoredProcedureFunc is the Go handler signature for in-memory stored
// procedures invoked with CALL name(...).
type StoredProcedureFunc = engine.StoredProcedureFunc

// StoredProcedureParameter describes one positional CALL argument.
type StoredProcedureParameter = engine.StoredProcedureParameter

// StoredProcedureOptions controls procedure metadata, argument validation,
// read concurrency and atomic rollback.
type StoredProcedureOptions = engine.StoredProcedureOptions

// StoredProcedureInfo describes one registered in-memory stored procedure.
type StoredProcedureInfo = engine.StoredProcedureInfo

// ExtensionCapability describes a resource category a statically linked Go
// extension declares it may need (filesystem, network, write, secrets).
//
// A declaration is self-reported, unverified metadata: tinySQL never checks
// it against what the extension's code actually does, and never will,
// because a statically linked Go extension already runs as ordinary code in
// the same process with the same privileges as the rest of the application —
// there is no language-level sandbox boundary between it and the host to
// enforce a capability against. Declarations exist so an operator can read
// sys.extensions (or ExtensionInfo.Capabilities) and review what an
// extension claims to need before calling db.Use, the same way you would
// read a new dependency's imports before adding it — not as an access
// control mechanism. Treat activating an extension exactly like adding any
// other trusted Go dependency: only activate code you trust.
type ExtensionCapability = storage.ExtensionCapability

const (
	CapabilityFilesystem = storage.CapabilityFilesystem
	CapabilityNetwork    = storage.CapabilityNetwork
	CapabilityWrite      = storage.CapabilityWrite
	CapabilitySecrets    = storage.CapabilitySecrets
)

// ExtensionInfo describes a Go extension active for one DB instance.
// Capabilities is the extension's own self-reported declaration -- see
// ExtensionCapability -- not a verified or enforced permission set.
type ExtensionInfo = storage.ExtensionInfo

// Extension is implemented by statically linked Go extension packages. Import
// an extension package in the embedding application and activate it with
// db.Use(extension). Dynamic Go shared-object plugins are intentionally not
// supported because they are not portable across tinySQL's Go, TinyGo, and
// WebAssembly targets.
//
// Register runs with the full privileges of the host process, unsandboxed,
// exactly like any other imported Go package's init-time code: db.Use is not
// a permission boundary, so only activate extensions whose source you trust.
type Extension = storage.Extension

// ExternalTableFunc is a simplified interface for registering table-valued
// functions from outside the tinySQL core. Unlike the internal TableFunction
// interface, it receives pre-evaluated argument values (string, int64,
// float64, bool, or nil) rather than AST expression nodes.
//
// This interface is the intended extension point for domain-specific layers
// (such as FSQL) that need to expose custom data sources as SQL tables.
//
// Example:
//
//	type MyFunc struct{}
//
//	func (f *MyFunc) Name() string { return "my_func" }
//	func (f *MyFunc) ValidateArgCount(n int) error {
//	    if n != 1 { return fmt.Errorf("my_func expects 1 argument") }
//	    return nil
//	}
//	func (f *MyFunc) Execute(ctx context.Context, args []any) (*tinysql.ResultSet, error) {
//	    // build and return ResultSet...
//	}
//
//	tinysql.RegisterExternalTableFunc(&MyFunc{})
type ExternalTableFunc interface {
	// Name returns the SQL function name (case-insensitive).
	Name() string

	// ValidateArgCount checks whether the number of arguments is acceptable.
	ValidateArgCount(n int) error

	// Execute evaluates the function with pre-evaluated argument values and
	// returns a ResultSet. The args slice contains one entry per SQL argument,
	// with values converted to Go native types (string, int64, float64, bool,
	// or nil).
	Execute(ctx context.Context, args []any) (*ResultSet, error)
}

// RegisterExternalTableFunc registers an ExternalTableFunc so it can be used
// in SQL FROM clauses and JOIN expressions. The function name is
// case-insensitive. Re-registering the same name replaces the previous
// registration.
func RegisterExternalTableFunc(fn ExternalTableFunc) {
	engine.RegisterExternalTableFunc(fn)
}

// RegisterStoredProcedure registers a process-local in-memory stored procedure.
// The procedure can be invoked from SQL with CALL name(arg1, arg2, ...).
// Registrations are not persisted in database snapshots.
func RegisterStoredProcedure(name string, fn StoredProcedureFunc) error {
	return engine.RegisterStoredProcedure(name, fn)
}

// RegisterStoredProcedureWithOptions registers a process-local procedure with
// argument metadata, read-only scheduling and optional atomic rollback.
func RegisterStoredProcedureWithOptions(name string, options StoredProcedureOptions, fn StoredProcedureFunc) error {
	return engine.RegisterStoredProcedureWithOptions(name, options, fn)
}

// UnregisterStoredProcedure removes a process-local stored procedure.
func UnregisterStoredProcedure(name string) bool {
	return engine.UnregisterStoredProcedure(name)
}

// ListStoredProcedures returns registered in-memory stored procedures sorted by name.
func ListStoredProcedures() []StoredProcedureInfo {
	return engine.ListStoredProcedures()
}
