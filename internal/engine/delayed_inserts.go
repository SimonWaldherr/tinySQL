package engine

import (
	"bytes"
	"container/heap"
	"crypto/rand"
	"encoding/gob"
	"encoding/hex"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// A normal persisted backing table, hidden from user-facing table catalogs.
// It is not a security boundary: embedders with direct storage access own it.
const delayedInsertTable = "__tiny_delayed_inserts"

type delayedInsertPayload struct {
	Version             int
	Table               string
	Columns             []string
	Schema              []storage.Column
	Values              [][]any
	OnConflictDoNothing bool
}

func init() {
	for _, proc := range []struct {
		name     string
		readOnly bool
		args     []string
		fn       StoredProcedureFunc
	}{
		{"DELAY_INSERT", false, []string{"when", "insert_sql"}, enqueueDelayedInsert},
		{"FLUSH_DELAYED_INSERTS", false, []string{"max_jobs"}, flushDelayedInserts},
		{"DELAYED_INSERTS", true, []string{}, listDelayedInserts},
		{"CANCEL_DELAYED_INSERT", false, []string{"id"}, cancelDelayedInsert},
		{"RESCHEDULE_DELAYED_INSERT", false, []string{"id", "when"}, rescheduleDelayedInsert},
	} {
		params := make([]StoredProcedureParameter, len(proc.args))
		for i, arg := range proc.args {
			params[i] = StoredProcedureParameter{Name: arg, Required: true}
		}
		if err := RegisterStoredProcedureWithOptions(proc.name, StoredProcedureOptions{ReadOnly: proc.readOnly, Atomic: !proc.readOnly, Parameters: params}, proc.fn); err != nil {
			panic(err)
		}
	}
}

func delayedInsertTime(value any, now time.Time) (time.Time, error) {
	if t, ok := value.(time.Time); ok && !t.IsZero() {
		return t.UTC(), nil
	}
	if s, ok := value.(string); ok {
		if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
			return t.UTC(), nil
		}
		if d, err := time.ParseDuration(s); err == nil && d >= 0 {
			return now.Add(d).UTC(), nil
		}
	}
	return time.Time{}, fmt.Errorf("expected a nonnegative duration (e.g. '10m') or RFC3339 timestamp")
}

func delayedQueue(pc ProcedureContext, create bool) (*storage.Table, error) {
	t, err := pc.env.db.Get(pc.env.tenant, delayedInsertTable)
	if err != nil && !errors.Is(err, storage.ErrTableNotFound) {
		return nil, err
	}
	if errors.Is(err, storage.ErrTableNotFound) {
		if !create {
			return nil, nil
		}
		stmt, err := NewParser(`CREATE TABLE __tiny_delayed_inserts (id TEXT PRIMARY KEY, due_at TIMESTAMP, target TEXT, payload BLOB, owner TEXT, created_at TIMESTAMP, row_count INT)`).ParseStatement()
		if err != nil {
			return nil, err
		}
		if _, err := execStmt(pc.env, stmt); err != nil {
			return nil, err
		}
		t, err = pc.env.db.Get(pc.env.tenant, delayedInsertTable)
		if err != nil {
			return nil, err
		}
	}
	names := []string{"id", "due_at", "target", "payload", "owner", "created_at", "row_count"}
	if len(t.Cols) != len(names) {
		return nil, fmt.Errorf("invalid delayed insert backing table")
	}
	for i, n := range names {
		if t.Cols[i].Name != n {
			return nil, fmt.Errorf("invalid delayed insert backing table")
		}
	}
	return t, nil
}

func enqueueDelayedInsert(pc ProcedureContext, args []any) (*ResultSet, error) {
	due, err := delayedInsertTime(args[0], pc.env.now)
	if err != nil {
		return nil, err
	}
	sql, ok := args[1].(string)
	if !ok {
		return nil, fmt.Errorf("insert_sql must be a string")
	}
	stmt, err := NewParser(sql).ParseStatement()
	if err != nil {
		return nil, err
	}
	insert, ok := stmt.(*Insert)
	if !ok || len(insert.Returning) != 0 {
		return nil, fmt.Errorf("DELAY_INSERT requires INSERT without RETURNING")
	}
	if strings.EqualFold(insert.Table, delayedInsertTable) {
		return nil, fmt.Errorf("cannot enqueue into the internal queue")
	}
	if err := checkPermission(pc.env.ctx, pc.env.db, insert); err != nil {
		return nil, err
	}
	target, err := pc.env.db.Get(pc.env.tenant, insert.Table)
	if err != nil {
		return nil, err
	}
	p := delayedInsertPayload{Version: 1, Table: insert.Table, Columns: insert.Cols, Schema: target.Cols, OnConflictDoNothing: insert.OnConflictDoNothing}
	rows := insert.Rows
	if insert.Select != nil {
		rs, err := pc.Execute(insert.Select)
		if err != nil {
			return nil, err
		}
		rows = insertRowsFromResultSet(rs)
	}
	expected := len(target.Cols)
	if len(insert.Cols) > 0 {
		expected = len(insert.Cols)
		seen := map[string]bool{}
		for _, col := range insert.Cols {
			key := strings.ToLower(col)
			if seen[key] {
				return nil, fmt.Errorf("duplicate insert column %q", col)
			}
			seen[key] = true
			found := false
			for _, c := range target.Cols {
				if strings.EqualFold(c.Name, col) {
					found = true
					break
				}
			}
			if !found {
				return nil, fmt.Errorf("unknown insert column %q", col)
			}
		}
	}
	for _, row := range rows {
		if err := checkCtx(pc.env.ctx); err != nil {
			return nil, err
		}
		if len(row) != expected {
			return nil, fmt.Errorf("INSERT expects %d values, got %d", expected, len(row))
		}
		values := make([]any, len(row))
		for i, expr := range row {
			values[i], err = evalExpr(pc.env, expr, Row{})
			if err != nil {
				return nil, err
			}
		}
		p.Values = append(p.Values, values)
	}
	var payload bytes.Buffer
	if err := gob.NewEncoder(&payload).Encode(p); err != nil {
		return nil, fmt.Errorf("encode delayed values: %w", err)
	}
	var idBytes [16]byte
	if _, err := rand.Read(idBytes[:]); err != nil {
		return nil, err
	}
	id := hex.EncodeToString(idBytes[:])
	if _, err := delayedQueue(pc, true); err != nil {
		return nil, err
	}
	owner, _ := UserFromContext(pc.env.ctx)
	queueInsert := &Insert{Table: delayedInsertTable, Rows: [][]Expr{{&Literal{Val: id}, &Literal{Val: due}, &Literal{Val: insert.Table}, &Literal{Val: payload.Bytes()}, &Literal{Val: owner}, &Literal{Val: pc.env.now}, &Literal{Val: len(rows)}}}}
	if _, err := execStmt(pc.env, queueInsert); err != nil {
		return nil, err
	}
	return &ResultSet{Cols: []string{"id", "due_at", "row_count"}, Rows: []Row{{"id": id, "due_at": due, "row_count": len(rows)}}}, nil
}

// Keep at most one batch of due-job references even with a large overdue queue.
// The latest candidate is the heap root, so an earlier job can replace it.
type delayedDueHeap [][]any

func (h delayedDueHeap) Len() int           { return len(h) }
func (h delayedDueHeap) Less(i, j int) bool { return h[i][1].(time.Time).After(h[j][1].(time.Time)) }
func (h delayedDueHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *delayedDueHeap) Push(v any)        { *h = append(*h, v.([]any)) }
func (h *delayedDueHeap) Pop() any          { old := *h; v := old[len(old)-1]; *h = old[:len(old)-1]; return v }

func flushDelayedInserts(pc ProcedureContext, args []any) (*ResultSet, error) {
	n, err := toInt(args[0])
	number, numeric := numericFast(args[0])
	if err != nil || !numeric || number != float64(n) || n < 1 || n > 1000 {
		return nil, fmt.Errorf("max_jobs must be an integer between 1 and 1000")
	}
	queue, err := delayedQueue(pc, false)
	if err != nil {
		return nil, err
	}
	var due delayedDueHeap
	if queue != nil {
		for _, row := range queue.Rows {
			if err := checkCtx(pc.env.ctx); err != nil {
				return nil, err
			}
			at, ok := row[1].(time.Time)
			if !ok {
				return nil, fmt.Errorf("invalid queue timestamp")
			}
			if !at.After(pc.env.now) {
				if len(due) < n {
					heap.Push(&due, row)
				} else if at.Before(due[0][1].(time.Time)) {
					due[0] = row
					heap.Fix(&due, 0)
				}
			}
		}
	}
	sort.SliceStable(due, func(i, j int) bool { return due[i][1].(time.Time).Before(due[j][1].(time.Time)) })
	if len(due) > n {
		due = due[:n]
	}
	for _, job := range due {
		var p delayedInsertPayload
		data, ok := job[3].([]byte)
		if !ok {
			return nil, fmt.Errorf("invalid delayed payload for %v", job[0])
		}
		if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&p); err != nil {
			return nil, err
		}
		if p.Version != 1 || strings.EqualFold(p.Table, delayedInsertTable) {
			return nil, fmt.Errorf("invalid delayed payload version or target")
		}
		target, err := pc.env.db.Get(pc.env.tenant, p.Table)
		if err != nil {
			return nil, err
		}
		if !reflect.DeepEqual(target.Cols, p.Schema) {
			return nil, fmt.Errorf("delayed insert %v: target schema changed", job[0])
		}
		insert := &Insert{Table: p.Table, Cols: p.Columns, OnConflictDoNothing: p.OnConflictDoNothing}
		for _, values := range p.Values {
			row := make([]Expr, len(values))
			for i, v := range values {
				row[i] = &Literal{Val: v}
			}
			insert.Rows = append(insert.Rows, row)
		}
		// Recheck the submitting identity as well as the worker's current
		// permissions, so revoking a grant also stops pending work.
		if pc.env.db.IsRBACEnabled() {
			owner, _ := job[4].(string)
			if err := checkPermission(WithUser(pc.env.ctx, owner), pc.env.db, insert); err != nil {
				return nil, fmt.Errorf("delayed insert %v: %w", job[0], err)
			}
		}
		if len(insert.Rows) > 0 {
			if _, err := pc.Execute(insert); err != nil {
				return nil, fmt.Errorf("delayed insert %v: %w", job[0], err)
			}
		}
		if _, err := execStmt(pc.env, &Delete{Table: delayedInsertTable, Where: &Binary{Op: "=", Left: &VarRef{Name: "id"}, Right: &Literal{Val: job[0]}}}); err != nil {
			return nil, err
		}
	}
	return &ResultSet{Cols: []string{"processed"}, Rows: []Row{{"processed": len(due)}}}, nil
}

func delayedJobVisible(pc ProcedureContext, row []any) bool {
	if !pc.env.db.IsRBACEnabled() {
		return true
	}
	user, _ := UserFromContext(pc.env.ctx)
	return row[4] == user
}

func listDelayedInserts(pc ProcedureContext, _ []any) (*ResultSet, error) {
	q, err := delayedQueue(pc, false)
	if err != nil {
		return nil, err
	}
	rs := &ResultSet{Cols: []string{"id", "due_at", "target", "created_at", "row_count"}}
	if q != nil {
		for _, row := range q.Rows {
			if delayedJobVisible(pc, row) {
				rs.Rows = append(rs.Rows, Row{"id": row[0], "due_at": row[1], "target": row[2], "created_at": row[5], "row_count": row[6]})
			}
		}
	}
	return rs, nil
}

func mutateDelayedInsert(pc ProcedureContext, args []any, reschedule bool) (*ResultSet, error) {
	id, ok := args[0].(string)
	if !ok {
		return nil, fmt.Errorf("id must be a string")
	}
	q, err := delayedQueue(pc, false)
	if err != nil {
		return nil, err
	}
	var found bool
	if q != nil {
		for _, row := range q.Rows {
			if row[0] == id && delayedJobVisible(pc, row) {
				found = true
				break
			}
		}
	}
	if !found {
		return nil, fmt.Errorf("delayed insert not found")
	}
	where := &Binary{Op: "=", Left: &VarRef{Name: "id"}, Right: &Literal{Val: id}}
	if reschedule {
		at, err := delayedInsertTime(args[1], pc.env.now)
		if err != nil {
			return nil, err
		}
		return execStmt(pc.env, &Update{Table: delayedInsertTable, Where: where, Sets: map[string]Expr{"due_at": &Literal{Val: at}}})
	}
	return execStmt(pc.env, &Delete{Table: delayedInsertTable, Where: where})
}
func cancelDelayedInsert(pc ProcedureContext, args []any) (*ResultSet, error) {
	return mutateDelayedInsert(pc, args, false)
}
func rescheduleDelayedInsert(pc ProcedureContext, args []any) (*ResultSet, error) {
	return mutateDelayedInsert(pc, args, true)
}
