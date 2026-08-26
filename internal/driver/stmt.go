// Prepared statements. A prepared query pre-parses its SQL once and rebinds
// only the parameter literals per execution, which is why the AST is reused
// under a pool rather than rebuilt.
package driver

import (
	"context"
	"database/sql/driver"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"sync"

	"github.com/SimonWaldherr/tinySQL/internal/engine"
)

type stmt struct {
	c        *conn
	sql      string
	prepared *preparedQuery
}

// preparedQuery is an immutable prepared-statement template. Each execution
// borrows an exclusive AST plus literal slots from pool, which avoids both
// reparsing on warm workers and the former statement-wide mutex. A streamed
// SELECT keeps its borrowed AST until the returned driver.Rows reaches EOF or
// Close, because engine.ExecuteStream can still dereference bound literals
// after QueryContext has returned.
type preparedQuery struct {
	markerSQL string
	markers   []string
	pool      sync.Pool
}

// preparedExecution is intentionally owned by exactly one goroutine between
// acquire and release. Keeping the bindable literals together with their AST
// makes concurrent prepared statements race-free without cloning the AST for
// every query. release restores markers so a pooled execution cannot retain
// caller BLOBs or other large parameter values.
type preparedExecution struct {
	statement engine.Statement
	params    []*engine.Literal
}

func (s *stmt) Close() error { return nil }

func (s *stmt) NumInput() int { return -1 }

func (s *stmt) Exec(args []driver.Value) (driver.Result, error) {
	n := make([]driver.NamedValue, len(args))
	for i, v := range args {
		n[i] = driver.NamedValue{Ordinal: i + 1, Value: v}
	}
	return s.ExecContext(context.Background(), n)
}

func (s *stmt) Query(args []driver.Value) (driver.Rows, error) {
	n := make([]driver.NamedValue, len(args))
	for i, v := range args {
		n[i] = driver.NamedValue{Ordinal: i + 1, Value: v}
	}
	return s.QueryContext(context.Background(), n)
}

func (s *stmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	if s.prepared != nil {
		return s.execPrepared(ctx, args)
	}
	sqlStr, err := bindPlaceholders(s.sql, args)
	if err != nil {
		return nil, err
	}
	return s.c.execSQL(ctx, sqlStr)
}

func (s *stmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	if s.prepared != nil {
		return s.queryPrepared(ctx, args)
	}
	sqlStr, err := bindPlaceholders(s.sql, args)
	if err != nil {
		return nil, err
	}
	return s.c.querySQL(ctx, sqlStr)
}

func (s *stmt) queryPrepared(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	if len(args) != len(s.prepared.markers) {
		return nil, fmt.Errorf("tinysql: expected %d placeholder arguments, got %d", len(s.prepared.markers), len(args))
	}
	exec, err := s.prepared.acquire()
	if err != nil {
		return nil, err
	}
	for i, arg := range args {
		exec.params[i].Val = driverValueLiteral(arg.Value)
	}

	// SELECTs can be consumed incrementally. Ownership of exec transfers to
	// the rows object, which releases it only after its stream has stopped.
	if !isWriteStatement(exec.statement) {
		return s.c.queryStatementWithCleanup(ctx, exec.statement, func() {
			s.prepared.release(exec)
		})
	}

	// Keep DML (including RETURNING) on its established writer/durability path.
	// The resulting rows are deliberately materialized: a write must complete
	// atomically and be persisted before database/sql observes any RETURNING
	// row.
	defer s.prepared.release(exec)
	if statementReturnsRows(exec.statement) {
		rs, err := s.c.executeWriteStatement(ctx, exec.statement)
		if err != nil {
			return nil, err
		}
		return &rows{rs: rs}, nil
	}
	if _, err := s.c.execStatement(ctx, exec.statement); err != nil {
		return nil, err
	}
	return emptyRows{}, nil
}

// execPrepared runs a prepared INSERT/UPDATE/DELETE straight off its pooled
// AST, mirroring queryPrepared's SELECT path. This is what lets a repeated
// prepared DML statement skip both the bindPlaceholders text render (which,
// for a []byte argument, otherwise hex-encodes it into a SQL literal only to
// have the parser immediately decode that hex back into bytes) and the full
// lex/parse pass that a text-based Exec always pays.
func (s *stmt) execPrepared(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	if len(args) != len(s.prepared.markers) {
		return nil, fmt.Errorf("tinysql: expected %d placeholder arguments, got %d", len(s.prepared.markers), len(args))
	}
	exec, err := s.prepared.acquire()
	if err != nil {
		return nil, err
	}
	defer s.prepared.release(exec)
	for i, arg := range args {
		exec.params[i].Val = driverValueLiteral(arg.Value)
	}
	return s.c.execStatement(ctx, exec.statement)
}

func driverValueLiteral(v any) any {
	// bindPlaceholders previously converted integer arguments into a decimal
	// SQL literal, which the parser represented as int where possible. Keep
	// that type behaviour so prepared index seeks use the same canonical keys.
	switch value := v.(type) {
	case int64:
		if int64(int(value)) == value {
			return int(value)
		}
		return value
	case []byte:
		return append([]byte(nil), value...)
	default:
		return value
	}
}

const preparedMarkerPrefix = "__tinysql_prepared_param_"

// buildPreparedQuery recognizes positional placeholders outside SQL strings
// and validates the marker form once. Numbered placeholders deliberately keep
// the text fallback: their repeated/reordered binding needs a separate ordinal
// map.
func buildPreparedQuery(sqlText string) (*preparedQuery, error) {
	markerSQL, count, ok := markerSQLForPositionalParams(sqlText)
	if !ok || count == 0 {
		return nil, nil
	}
	markers := make([]string, count)
	for i := range markers {
		markers[i] = preparedMarkerPrefix + strconv.Itoa(i) + "__"
	}
	prepared := &preparedQuery{markerSQL: markerSQL, markers: markers}
	// Parse and validate once at Prepare time. Seeding the pool gives the
	// common single-goroutine path the original no-reparse behavior; additional
	// workers create isolated executions only when needed.
	exec, err := prepared.newExecution()
	if err != nil {
		return nil, err
	}
	prepared.pool.Put(exec)
	return prepared, nil
}

func (p *preparedQuery) acquire() (*preparedExecution, error) {
	if value := p.pool.Get(); value != nil {
		return value.(*preparedExecution), nil
	}
	return p.newExecution()
}

func (p *preparedQuery) release(exec *preparedExecution) {
	for i, literal := range exec.params {
		literal.Val = p.markers[i]
	}
	p.pool.Put(exec)
}

func (p *preparedQuery) newExecution() (*preparedExecution, error) {
	statement, err := engine.NewParser(p.markerSQL).ParseStatement()
	if err != nil {
		return nil, err
	}
	// SELECT borrows an AST until its rows stream is closed; DML remains fully
	// materialized before release. INSERT/UPDATE/DELETE are read-only over
	// their own AST during execution
	// (executeInsert/executeUpdate/executeDelete only ever read s.Rows/
	// s.Sets/s.Where to evaluate values and rebind via evalExpr; nothing
	// mutates the statement node itself), so the same acquire/rebind-
	// literals/release cycle that already makes concurrent SELECT reuse
	// race-free is equally safe for them. Any other statement kind (DDL,
	// transaction control, etc.) keeps the text-parse fallback.
	switch statement.(type) {
	case *engine.Select, *engine.Insert, *engine.Update, *engine.Delete:
	default:
		return nil, fmt.Errorf("tinysql: prepared fast path supports SELECT/INSERT/UPDATE/DELETE only")
	}
	markerPositions := make(map[string]int, len(p.markers))
	for i, marker := range p.markers {
		markerPositions[marker] = i
	}
	params := make([]*engine.Literal, len(p.markers))
	collectPreparedLiterals(reflect.ValueOf(statement), markerPositions, params)
	for i, literal := range params {
		if literal == nil {
			return nil, fmt.Errorf("tinysql: positional parameter %d was not parsed as a literal", i+1)
		}
		literal.Parameter = true
	}
	return &preparedExecution{statement: statement, params: params}, nil
}

func markerSQLForPositionalParams(sqlText string) (string, int, bool) {
	var out strings.Builder
	out.Grow(len(sqlText) + 32)
	count := 0
	for i := 0; i < len(sqlText); i++ {
		ch := sqlText[i]
		if ch == '\'' {
			out.WriteByte(ch)
			i++
			for i < len(sqlText) {
				b := sqlText[i]
				out.WriteByte(b)
				if b == '\'' {
					if i+1 < len(sqlText) && sqlText[i+1] == '\'' {
						i++
						out.WriteByte(sqlText[i])
						i++
						continue
					}
					break
				}
				i++
			}
			continue
		}
		if ch == '?' {
			out.WriteByte('\'')
			out.WriteString(preparedMarkerPrefix)
			out.WriteString(strconv.Itoa(count))
			out.WriteString("__'")
			count++
			continue
		}
		// Keep $1/:1 on the established text-binding path. This also avoids
		// interpreting PostgreSQL casts or identifiers containing a colon.
		if ch == '$' || ch == ':' {
			if i+1 < len(sqlText) && sqlText[i+1] >= '0' && sqlText[i+1] <= '9' {
				return "", 0, false
			}
		}
		out.WriteByte(ch)
	}
	return out.String(), count, true
}

var literalPtrType = reflect.TypeOf((*engine.Literal)(nil))

func collectPreparedLiterals(v reflect.Value, markers map[string]int, params []*engine.Literal) {
	if !v.IsValid() {
		return
	}
	if v.Type() == literalPtrType {
		if v.IsNil() {
			return
		}
		literal := v.Interface().(*engine.Literal)
		if marker, ok := literal.Val.(string); ok {
			if i, ok := markers[marker]; ok {
				params[i] = literal
			}
		}
		return
	}
	switch v.Kind() {
	case reflect.Interface, reflect.Pointer:
		if !v.IsNil() {
			collectPreparedLiterals(v.Elem(), markers, params)
		}
	case reflect.Struct:
		for i := 0; i < v.NumField(); i++ {
			if v.Field(i).CanInterface() {
				collectPreparedLiterals(v.Field(i), markers, params)
			}
		}
	case reflect.Slice, reflect.Array:
		for i := 0; i < v.Len(); i++ {
			collectPreparedLiterals(v.Index(i), markers, params)
		}
	case reflect.Map:
		// engine.Update.Sets is a map[string]Expr, so a placeholder in a SET
		// clause ("SET col = ?") only surfaces through this case. Map values
		// read via MapIndex are not addressable, but that is fine here: the
		// literal we need is the *engine.Literal pointer itself (the value
		// stored in the interface), not a field to write through the map --
		// literal.Val is mutated by acquire()/release() through that pointer,
		// which is the same object the map's Sets entry holds either way.
		for _, k := range v.MapKeys() {
			collectPreparedLiterals(v.MapIndex(k), markers, params)
		}
	}
}
