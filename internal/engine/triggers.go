// Package engine implements trigger execution for tinySQL.
// Triggers fire BEFORE or AFTER INSERT, UPDATE, and DELETE operations.
// NEW refers to the new row (INSERT/UPDATE), OLD refers to the old row (UPDATE/DELETE).
package engine

import (
	"fmt"
	"strings"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
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
		Body:       s.BodyText,
	}

	if err := cat.RegisterTrigger(t); err != nil {
		return nil, err
	}
	return nil, nil
}

// executeDropTrigger removes a trigger from the catalog.
func executeDropTrigger(env ExecEnv, s *DropTrigger) (*ResultSet, error) {
	err := env.db.Catalog().DropTrigger(s.Name)
	if err != nil && !s.IfExists {
		return nil, err
	}
	return nil, nil
}

// fireTriggers executes all matching triggers for the given table/timing/event.
// newRow contains the NEW pseudo-row values (for INSERT/UPDATE).
// oldRow contains the OLD pseudo-row values (for UPDATE/DELETE).
func fireTriggers(env ExecEnv, table string, timing string, event string, newRow Row, oldRow Row) error {
	triggers := env.db.Catalog().GetTriggers(table,
		storage.TriggerTiming(timing),
		storage.TriggerEvent(event))
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
	// Build an enriched row with new.col and old.col
	trigRow := make(Row)
	for k, v := range newRow {
		trigRow["new."+k] = v
		trigRow[k] = v
	}
	for k, v := range oldRow {
		trigRow["old."+k] = v
	}

	// Parse the trigger body SQL statements.
	stmts, err := parseTriggerBody(trig.Body)
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
	// TODO: evaluate WHEN condition against trigRow when that feature is added.
	return nil
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

