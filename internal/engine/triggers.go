// Package engine implements trigger execution for tinySQL.
// Triggers fire BEFORE or AFTER INSERT, UPDATE, and DELETE operations.
// NEW refers to the new row (INSERT/UPDATE), OLD refers to the old row (UPDATE/DELETE).
package engine

import (
	"fmt"
	"strings"
	"sync"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

type triggerCacheEntry struct {
	body  string
	stmts []Statement
}

// triggerWhenCacheEntry pairs a compiled WHEN expression with the raw text it
// was parsed from, so a cache hit keyed by trigger name can detect a
// redefinition (same name, new WHEN clause) the way triggerCacheEntry.body
// already does for trigger bodies.
type triggerWhenCacheEntry struct {
	text string
	expr Expr
}

// triggerCacheMaxEntries bounds both caches below, mirroring regexCache: keyed
// by trigger name, they are purged precisely on DROP TRIGGER (see
// executeDropTrigger), but this cap is a backstop against unbounded growth
// from deployments that keep churning through distinct trigger names without
// ever dropping the old ones.
const (
	triggerCacheMaxEntries = 256
	// maxTriggerDepth prevents a self-referential trigger chain from growing
	// the Go stack until the process fails. Nested triggers share ExecEnv, so
	// the limit covers both direct and indirect recursion.
	maxTriggerDepth = 32
)

var (
	triggerCacheMu   sync.RWMutex
	triggerBodyCache = make(map[string]triggerCacheEntry)
	triggerWhenCache = make(map[string]triggerWhenCacheEntry)
)

// executeCreateTrigger stores a trigger definition in the catalog.
func executeCreateTrigger(env ExecEnv, s *CreateTrigger) (*ResultSet, error) {
	cat := env.db.Catalog()
	if s.IfNotExists {
		existing := cat.ListTriggers()
		for _, t := range existing {
			if strings.EqualFold(t.Name, s.Name) {
				return nil, nil
			}
		}
	}

	t := &storage.CatalogTrigger{
		Name:       s.Name,
		Table:      s.Table,
		Timing:     storage.TriggerTiming(s.Timing),
		Event:      storage.TriggerEvent(s.Event),
		ForEachRow: s.ForEachRow,
		WhenExpr:   s.WhenText,
		Body:       s.BodyText,
	}

	if err := cat.RegisterTrigger(t); err != nil {
		return nil, err
	}
	cacheTriggerBody(t.Name, t.Body, s.Body)
	if s.WhenExpr != nil && s.WhenText != "" {
		cacheTriggerWhen(t.Name, t.WhenExpr, s.WhenExpr)
	}
	return nil, nil
}

// executeDropTrigger removes a trigger from the catalog.
func executeDropTrigger(env ExecEnv, s *DropTrigger) (*ResultSet, error) {
	err := env.db.Catalog().DropTrigger(s.Name)
	if err != nil && !s.IfExists {
		return nil, err
	}
	// Purge the per-trigger cache entries regardless of outcome: on success
	// they are now stale, and on an IfExists no-op there is nothing to purge
	// so the delete is a harmless no-op. Without this, triggerBodyCache and
	// triggerWhenCache grow by one entry per distinct trigger name ever
	// created in a long-running deployment that creates/drops triggers
	// dynamically.
	triggerCacheMu.Lock()
	delete(triggerBodyCache, s.Name)
	delete(triggerWhenCache, s.Name)
	triggerCacheMu.Unlock()
	return nil, nil
}

// fireTriggers executes all matching triggers for the given table/timing/event.
// newRow contains the NEW pseudo-row values (for INSERT/UPDATE).
// oldRow contains the OLD pseudo-row values (for UPDATE/DELETE).
func fireTriggers(env ExecEnv, table string, timing string, event string, newRow Row, oldRow Row) error {
	before, after := env.db.Catalog().GetTriggersForEvent(table, storage.TriggerEvent(event))
	var triggers []*storage.CatalogTrigger
	switch storage.TriggerTiming(timing) {
	case storage.TriggerBefore:
		triggers = before
	case storage.TriggerAfter:
		triggers = after
	}
	return fireTriggerList(env, triggers, newRow, oldRow)
}

// fireTriggerList runs a list resolved before the DML row loop. Trigger
// definitions cannot change while the outer statement holds the write lock,
// so reusing this list prevents catalog scans and slice allocations per row.
func fireTriggerList(env ExecEnv, triggers []*storage.CatalogTrigger, newRow Row, oldRow Row) error {
	runner := triggerListRunner{triggers: triggers}
	return runner.fire(env, newRow, oldRow)
}

type preparedTrigger struct {
	trigger *storage.CatalogTrigger
	when    Expr
	stmts   []Statement
}

// triggerListRunner resolves cached WHEN/body programs on the first affected
// row and reuses them for the rest of the statement. This preserves the old
// behavior that an invalid trigger is not parsed when zero rows match, while
// removing two cache locks per trigger from every subsequent row.
type triggerListRunner struct {
	triggers []*storage.CatalogTrigger
	programs []preparedTrigger
	inline   [2]preparedTrigger
}

func (r *triggerListRunner) prepare() error {
	programs := r.inline[:0]
	if len(r.triggers) <= cap(programs) {
		programs = programs[:len(r.triggers)]
	} else {
		programs = make([]preparedTrigger, len(r.triggers))
	}
	for i, trigger := range r.triggers {
		programs[i].trigger = trigger
		if strings.TrimSpace(trigger.WhenExpr) != "" {
			when, err := triggerWhenExpr(trigger.Name, trigger.WhenExpr)
			if err != nil {
				return fmt.Errorf("trigger %q: %w", trigger.Name, err)
			}
			programs[i].when = when
		}
		stmts, err := triggerBodyStatements(trigger)
		if err != nil {
			return fmt.Errorf("trigger %q: %w", trigger.Name, err)
		}
		programs[i].stmts = stmts
	}
	r.programs = programs
	return nil
}

func (r *triggerListRunner) fire(env ExecEnv, newRow Row, oldRow Row) error {
	if len(r.triggers) == 0 {
		return nil
	}
	if r.programs == nil {
		if err := r.prepare(); err != nil {
			return err
		}
	}
	binding := triggerRowBinding{newRow: newRow, oldRow: oldRow}
	for i := range r.programs {
		program := &r.programs[i]
		if err := executePreparedTrigger(env, program, &binding); err != nil {
			return fmt.Errorf("trigger %q: %w", program.trigger.Name, err)
		}
	}
	return nil
}

// triggerRowBinding lets NEW.<col>/OLD.<col>/bare-<col> references inside a
// trigger's WHEN clause and body resolve directly against the newRow/oldRow
// maps the firing INSERT/UPDATE/DELETE already built for its own WHERE and
// RETURNING evaluation, instead of copying every one of their (already
// duplicated, table-qualified-plus-bare) keys into a third map under
// new./old.-renamed keys on every single trigger firing.
//
// newRow and oldRow are themselves produced by buildTableRow, so each already
// maps both a column's bare lowercased name and its table-qualified name to
// the same value; lookupTriggerRow strips a leading "new."/"old." and reuses
// that existing key space instead of re-deriving it.
type triggerRowBinding struct {
	newRow Row
	oldRow Row
}

// lookupTriggerRow resolves a lowercased column reference against a trigger's
// NEW/OLD bindings exactly as the old copied-map shape did: "new."-prefixed
// and bare references resolve against newRow (bare defaults to NEW, matching
// SQL trigger convention — OLD requires the explicit prefix), and
// "old."-prefixed references resolve against oldRow. A nil newRow or oldRow
// (INSERT has no OLD row, DELETE has no NEW row) is a no-op lookup, matching
// ranging over a nil map in the old implementation.
func lookupTriggerRow(tb *triggerRowBinding, lower string) (any, bool) {
	switch {
	case strings.HasPrefix(lower, "new."):
		return getValLower(tb.newRow, lower[len("new."):])
	case strings.HasPrefix(lower, "old."):
		return getValLower(tb.oldRow, lower[len("old."):])
	default:
		return getValLower(tb.newRow, lower)
	}
}

// triggerRowSuggestionRow reconstructs the merged new./old./bare-keyed map
// the old implementation always built, purely to source "did you mean...?"
// candidates on the cold unknown-column-error path — see
// columnSuggestionFromRow. Correctness there matters more than allocating,
// and this path is only ever reached once a query has already failed.
func triggerRowSuggestionRow(tb *triggerRowBinding) Row {
	row := make(Row, len(tb.newRow)*2+len(tb.oldRow))
	for k, v := range tb.newRow {
		row["new."+k] = v
		row[k] = v
	}
	for k, v := range tb.oldRow {
		row["old."+k] = v
	}
	return row
}

// executeTrigger runs a single trigger's body in an enriched environment that
// exposes NEW.<col> and OLD.<col> pseudo-columns.
func executePreparedTrigger(env ExecEnv, program *preparedTrigger, binding *triggerRowBinding) error {
	if env.triggerDepth >= maxTriggerDepth {
		return fmt.Errorf("maximum trigger nesting depth (%d) exceeded", maxTriggerDepth)
	}
	env.triggerDepth++

	// Bind NEW.col/OLD.col/bare-col lookups directly against newRow/oldRow
	// (see triggerRowBinding) rather than pre-copying every key into a third
	// map. Set before the WHEN check too, so WHEN and the body resolve
	// columns the same way (evalVarRef falls back to env.triggerRow whenever
	// its row argument doesn't have the reference — passing nil below for
	// WHEN's row argument routes it through that same fallback).
	env.triggerRow = binding

	if program.when != nil {
		ok, err := evalExpr(env, program.when, nil)
		if err != nil {
			return err
		}
		if toTri(ok) != tvTrue {
			return nil
		}
	}

	for _, stmt := range program.stmts {
		// execStmt, not Execute: trigger bodies run inside the INSERT/UPDATE/
		// DELETE that fired them, already inside Execute's write lock on the
		// same goroutine — re-acquiring it here would deadlock (sync.RWMutex
		// is not reentrant).
		if _, err := execStmt(env, stmt); err != nil {
			return err
		}
	}
	return nil
}

func cacheTriggerBody(name, body string, stmts []Statement) {
	triggerCacheMu.Lock()
	defer triggerCacheMu.Unlock()
	if _, exists := triggerBodyCache[name]; !exists && len(triggerBodyCache) >= triggerCacheMaxEntries {
		// Simple full reset: bounded memory without LRU bookkeeping, same
		// tradeoff regexCache makes. DROP TRIGGER already purges the common
		// case (a trigger actually being retired); this only guards against
		// deployments that keep defining new trigger names without ever
		// dropping the old ones.
		triggerBodyCache = make(map[string]triggerCacheEntry)
	}
	triggerBodyCache[name] = triggerCacheEntry{body: body, stmts: append([]Statement(nil), stmts...)}
}

func triggerBodyStatements(trig *storage.CatalogTrigger) ([]Statement, error) {
	triggerCacheMu.RLock()
	if cached, ok := triggerBodyCache[trig.Name]; ok && cached.body == trig.Body {
		triggerCacheMu.RUnlock()
		// The cached slice and parsed ASTs are read-only during execution;
		// query-plan caches embedded in statements synchronize their own
		// mutable state. Returning it directly removes one allocation for
		// every fired trigger.
		return cached.stmts, nil
	}
	triggerCacheMu.RUnlock()

	stmts, err := parseTriggerBody(trig.Body)
	if err != nil {
		return nil, err
	}
	cacheTriggerBody(trig.Name, trig.Body, stmts)
	return stmts, nil
}

// cacheTriggerWhen caches a compiled WHEN expression under the owning
// trigger's name (rather than the raw WHEN text) so executeDropTrigger can
// purge it directly, the same way it purges triggerBodyCache.
func cacheTriggerWhen(name, text string, expr Expr) {
	triggerCacheMu.Lock()
	defer triggerCacheMu.Unlock()
	if _, exists := triggerWhenCache[name]; !exists && len(triggerWhenCache) >= triggerCacheMaxEntries {
		// See cacheTriggerBody: same bounded-reset backstop as regexCache.
		triggerWhenCache = make(map[string]triggerWhenCacheEntry)
	}
	triggerWhenCache[name] = triggerWhenCacheEntry{text: text, expr: expr}
}

func triggerWhenExpr(name, text string) (Expr, error) {
	text = strings.TrimSpace(text)
	triggerCacheMu.RLock()
	if cached, ok := triggerWhenCache[name]; ok && cached.text == text {
		triggerCacheMu.RUnlock()
		return cached.expr, nil
	}
	triggerCacheMu.RUnlock()

	p := NewParser(text)
	expr, err := p.parseExpr()
	if err != nil {
		return nil, fmt.Errorf("trigger WHEN parse: %w", err)
	}
	cacheTriggerWhen(name, text, expr)
	return expr, nil
}

// parseTriggerBody splits and parses semicolon-separated SQL statements.
func parseTriggerBody(body string) ([]Statement, error) {
	var stmts []Statement
	for _, raw := range strings.Split(body, ";") {
		raw = strings.TrimSpace(raw)
		if raw == "" {
			continue
		}
		p := NewParser(raw)
		stmt, err := p.ParseStatement()
		if err != nil {
			return nil, fmt.Errorf("trigger body parse: %w", err)
		}
		stmts = append(stmts, stmt)
	}
	return stmts, nil
}
