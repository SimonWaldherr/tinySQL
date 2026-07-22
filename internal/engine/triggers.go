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
	if len(triggers) == 0 {
		return nil
	}

	for _, trig := range triggers {
		if err := executeTrigger(env, trig, newRow, oldRow); err != nil {
			return fmt.Errorf("trigger %q: %w", trig.Name, err)
		}
	}
	return nil
}

// executeTrigger runs a single trigger's body in an enriched environment that
// exposes NEW.<col> and OLD.<col> pseudo-columns.
func executeTrigger(env ExecEnv, trig *storage.CatalogTrigger, newRow Row, oldRow Row) error {
	if env.triggerDepth >= maxTriggerDepth {
		return fmt.Errorf("maximum trigger nesting depth (%d) exceeded", maxTriggerDepth)
	}
	env.triggerDepth++

	// Build an enriched row with new.col and old.col
	trigRow := make(Row)
	for k, v := range newRow {
		trigRow["new."+k] = v
		trigRow[k] = v
	}
	for k, v := range oldRow {
		trigRow["old."+k] = v
	}

	if strings.TrimSpace(trig.WhenExpr) != "" {
		whenExpr, err := triggerWhenExpr(trig.Name, trig.WhenExpr)
		if err != nil {
			return err
		}
		ok, err := evalExpr(env, whenExpr, trigRow)
		if err != nil {
			return err
		}
		if toTri(ok) != tvTrue {
			return nil
		}
	}

	stmts, err := triggerBodyStatements(trig)
	if err != nil {
		return err
	}

	// Make NEW.col/OLD.col resolvable inside the body statements themselves
	// (e.g. "INSERT INTO audit_log VALUES (NEW.id, ...)") via evalVarRef's
	// env.triggerRow fallback.
	env.triggerRow = trigRow

	for _, stmt := range stmts {
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
