package engine

import (
	"context"
	"crypto/sha256"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// IndexAdvisorOptions bounds opt-in workload observation and index creation.
// Zero limits use defaults. Automatic creation is disabled by default.
type IndexAdvisorOptions struct {
	AutoCreate         bool
	MinExecutions      int // default 20 successful SELECTs
	MinTableRows       int // default 1024
	MaxTableRows       int // default 100000; checked again under the DDL write lock
	MaxIndexesPerTable int // default 3, including manually created indexes
	MaxCandidates      int // default 256 across tenants/tables/columns
}

// IndexRecommendation contains aggregate counters, never SQL or literal values.
// Status is collecting, ready, covered, created, skipped or failed.
type IndexRecommendation struct {
	Tenant, Table, Column, Name string
	Executions                  uint64
	TotalDuration               time.Duration // query wall time, including lock wait
	TableRows, SampleDistinct   int
	Status, LastError           string
}

type IndexAdvisor struct {
	mu         sync.Mutex
	db         *storage.DB
	options    IndexAdvisorOptions
	candidates map[string]*IndexRecommendation
}

// NewIndexAdvisor creates a DB-scoped observer. Only queries executed through
// its Execute method are observed; the ordinary Execute path has no overhead.
func NewIndexAdvisor(db *storage.DB, options IndexAdvisorOptions) (*IndexAdvisor, error) {
	if db == nil {
		return nil, fmt.Errorf("index advisor requires a database")
	}
	limits := []struct {
		value    *int
		fallback int
	}{{&options.MinExecutions, 20}, {&options.MinTableRows, 1024}, {&options.MaxTableRows, 100000}, {&options.MaxIndexesPerTable, 3}, {&options.MaxCandidates, 256}}
	for _, limit := range limits {
		if *limit.value < 0 {
			return nil, fmt.Errorf("index advisor limits must be nonnegative")
		}
		if *limit.value == 0 {
			*limit.value = limit.fallback
		}
	}
	if options.MaxTableRows < options.MinTableRows {
		return nil, fmt.Errorf("index advisor max rows is below min rows")
	}
	return &IndexAdvisor{db: db, options: options, candidates: make(map[string]*IndexRecommendation)}, nil
}

// SetAutoCreate changes the opt-in mode. Enabling it does not immediately build
// pending recommendations; the next observed eligible query can build one.
func (a *IndexAdvisor) SetAutoCreate(enabled bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.options.AutoCreate = enabled
}

// Recommendations returns a detached, deterministically ordered snapshot.
func (a *IndexAdvisor) Recommendations() []IndexRecommendation {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := make([]IndexRecommendation, 0, len(a.candidates))
	for _, r := range a.candidates {
		out = append(out, *r)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Tenant != out[j].Tenant {
			return out[i].Tenant < out[j].Tenant
		}
		return out[i].Name < out[j].Name
	})
	return out
}

// Execute observes successful simple SELECT equality filters. If enabled, at
// most one non-unique index is built synchronously after the query completes.
// A build failure is recorded in LastError, never substituted for a successful
// query result. Use Apply to receive build errors directly. There is no worker.
func (a *IndexAdvisor) Execute(ctx context.Context, tenant string, stmt Statement) (*ResultSet, error) {
	start := time.Now()
	rs, err := Execute(ctx, a.db, tenant, stmt)
	if err != nil {
		return rs, err
	}
	elapsed := time.Since(start)
	columns, table := indexAdvisorColumns(stmt)
	if len(columns) == 0 {
		return rs, nil
	}
	if tenant == "" {
		tenant = "default"
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	built := false
	for _, column := range columns {
		digest := sha256.Sum256([]byte(table + "\x00" + column))
		name := fmt.Sprintf("auto_idx_%x", digest[:12])
		key := tenant + "\x00" + name
		r := a.candidates[key]
		if r == nil {
			if len(a.candidates) >= a.options.MaxCandidates {
				continue
			}
			r = &IndexRecommendation{Tenant: tenant, Table: table, Column: column, Name: name, Status: "collecting"}
			a.candidates[key] = r
		}
		r.Executions++
		r.TotalDuration += elapsed
		if r.Status == "created" || r.Status == "failed" {
			continue
		}
		if r.Executions < uint64(a.options.MinExecutions) {
			continue
		}
		if r.Status == "ready" {
			if a.options.AutoCreate && !built {
				_ = a.applyLocked(ctx, r)
				built = true
			}
			continue
		}
		if r.Status != "collecting" && r.Executions%uint64(a.options.MinExecutions) != 0 {
			continue
		}
		a.db.LockContentForRead()
		status, rows, distinct, reason := indexAdvisorCheck(a.db, r, a.options)
		a.db.UnlockContentForRead()
		r.Status, r.TableRows, r.SampleDistinct, r.LastError = status, rows, distinct, reason
		if status == "ready" && a.options.AutoCreate && !built {
			_ = a.applyLocked(ctx, r)
			built = true
		}
	}
	return rs, nil
}

// Apply builds a known, ready recommendation using ordinary CREATE INDEX
// authorization, rollback, auditing and durability. Limits and schema are
// rechecked under that statement's content write lock.
func (a *IndexAdvisor) Apply(ctx context.Context, tenant, name string) error {
	if tenant == "" {
		tenant = "default"
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	r := a.candidates[tenant+"\x00"+name]
	if r == nil {
		return fmt.Errorf("unknown index recommendation %q", name)
	}
	if r.Executions < uint64(a.options.MinExecutions) {
		return fmt.Errorf("index recommendation has insufficient observations")
	}
	return a.applyLocked(ctx, r)
}

func (a *IndexAdvisor) applyLocked(ctx context.Context, r *IndexRecommendation) error {
	guard := &indexAdvisorBuildGuard{recommendation: *r, options: a.options}
	_, err := Execute(ctx, a.db, r.Tenant, &CreateIndex{Name: r.Name, Table: r.Table, Columns: []string{r.Column}, advisor: guard})
	if err != nil {
		r.Status = "failed"
		r.LastError = err.Error()
		return err
	}
	if guard.covered && r.Status != "created" {
		r.Status = "covered"
	} else {
		r.Status = "created"
	}
	r.LastError = ""
	return nil
}

type indexAdvisorBuildGuard struct {
	recommendation IndexRecommendation
	options        IndexAdvisorOptions
	covered        bool
}

// Called with the DB content lock held. The bounded, evenly spaced sample is
// a cardinality heuristic, not a cost model: low-cardinality filters are left
// alone. No literal values or sample keys are retained in the advisor.
func indexAdvisorCheck(db *storage.DB, r *IndexRecommendation, o IndexAdvisorOptions) (string, int, int, string) {
	t, err := db.Get(r.Tenant, r.Table)
	if err != nil {
		return "skipped", 0, 0, err.Error()
	}
	pos, err := t.ColIndex(r.Column)
	if err != nil {
		return "skipped", len(t.Rows), 0, err.Error()
	}
	n := len(t.Rows)
	if t.Cols[pos].Constraint == storage.PrimaryKey || t.Cols[pos].Constraint == storage.Unique {
		return "covered", n, 0, "column already has a uniqueness constraint"
	}
	for _, idx := range t.Indexes {
		if len(idx.Columns) > 0 && strings.EqualFold(idx.Columns[0], r.Column) {
			return "covered", n, 0, "existing index has this leading column"
		}
	}
	if len(t.Indexes) >= o.MaxIndexesPerTable {
		return "skipped", n, 0, "table index limit reached"
	}
	if n < o.MinTableRows || n > o.MaxTableRows {
		return "skipped", n, 0, "table row count outside configured limits"
	}
	sample := min(n, 256)
	distinct := make(map[string]struct{}, sample)
	for i := 0; i < sample; i++ {
		row := t.Rows[i*n/sample]
		if pos >= len(row) || row[pos] == nil {
			continue
		}
		if value, ok := row[pos].(string); ok && len(value) > 1024 {
			return "skipped", n, 0, "sample contains text longer than 1024 bytes"
		}
		switch row[pos].(type) {
		case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64, string, bool:
		default:
			return "skipped", n, 0, "sample contains unsupported index value types"
		}
		distinct[string(storage.CanonicalIndexKey([]any{row[pos]}))] = struct{}{}
	}
	if len(distinct) < 16 {
		return "skipped", n, len(distinct), "sample has fewer than 16 distinct non-NULL values"
	}
	return "ready", n, len(distinct), ""
}

func indexAdvisorColumns(stmt Statement) ([]string, string) {
	s, ok := stmt.(*Select)
	if !ok || s.From.Table == "" || s.From.Subquery != nil || s.From.TableFunc != nil || len(s.Joins) > 0 || len(s.CTEs) > 0 || s.Union != nil {
		return nil, ""
	}
	table := strings.ToLower(s.From.Table)
	columns := make(map[string]struct{})
	var visit func(Expr)
	visit = func(e Expr) {
		b, ok := e.(*Binary)
		if !ok {
			return
		}
		if strings.EqualFold(b.Op, "AND") {
			visit(b.Left)
			visit(b.Right)
			return
		}
		if b.Op != "=" {
			return
		}
		v, ok := b.Left.(*VarRef)
		lit, lok := b.Right.(*Literal)
		if !ok || !lok {
			v, ok = b.Right.(*VarRef)
			lit, lok = b.Left.(*Literal)
		}
		if !ok || !lok || lit.Val == nil {
			return
		}
		col := strings.ToLower(v.Name)
		if dot := strings.LastIndexByte(col, '.'); dot >= 0 {
			prefix := col[:dot]
			if prefix != table && (s.From.Alias == "" || prefix != strings.ToLower(s.From.Alias)) {
				return
			}
			col = col[dot+1:]
		}
		columns[col] = struct{}{}
	}
	visit(s.Where)
	out := make([]string, 0, len(columns))
	for col := range columns {
		out = append(out, col)
	}
	sort.Strings(out)
	return out, table
}
