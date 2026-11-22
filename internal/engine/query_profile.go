package engine

import (
	"context"
	"time"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// QueryProfile holds elapsed wall-clock timings for one execution.
// Elapsed covers the statement lifecycle, including lock acquisition and
// finalization. ParseTime is separate and zero when executing a parsed statement.
type QueryProfile struct {
	ParseTime time.Duration
	Elapsed   time.Duration
	Stages    []QueryStage
}

// QueryStage aggregates calls at the same path in an execution. Parent is the
// one-based index in QueryProfile.Stages of the enclosing stage, or zero for
// the execution root. Rows sums reported outputs across all calls. Total
// includes measured children and Self excludes them.
type QueryStage struct {
	Operation string
	Detail    string
	Parent    int
	Calls     int64
	Rows      int64
	Total     time.Duration
	Self      time.Duration
}

// ExecuteWithProfile executes a parsed statement through the normal statement
// lifecycle and returns its result and timings. On error the profile contains
// the stages reached before the failure. Statement execution is measured once.
func ExecuteWithProfile(ctx context.Context, db *storage.DB, tenant string, stmt Statement) (*ResultSet, QueryProfile, error) {
	profile := newQueryProfile()
	profile.statementLifecycle = true
	root := profile.start(0, "EXECUTE", statementName(stmt))
	result, err := executeStatementWithColumnsAndProfile(ctx, db, tenant, stmt, nil, profile)
	// The ordinary executor has returned after its deferred cleanup and lock
	// release. Finish before materializing the timing report.
	root.finish(profileResultRows(result))
	return result, profile.report(), err
}

// The executor runs a statement's stages synchronously. A collector belongs
// to one execution, never to a cached plan or AST. Repeated calls at the same
// path (for example recursive CTE iterations) accumulate instead of retaining a trace
// entry per input row.
type queryProfile struct {
	clock              func() time.Time
	index              map[queryPhaseKey]int
	nodes              []queryPhaseStats
	statementLifecycle bool
	active             *queryPhase
}

type queryPhaseKey struct {
	parent            int
	operation, detail string
}

type queryPhaseStats struct {
	queryPhaseKey
	calls, rows     int64
	total, children time.Duration
}

type queryPhase struct {
	profile  *queryProfile
	previous *queryPhase
	index    int
	started  time.Time
}

func newQueryProfile() *queryProfile {
	return &queryProfile{clock: time.Now, index: make(map[queryPhaseKey]int)}
}

func startQueryPhase(env ExecEnv, operation, detail string) (ExecEnv, *queryPhase) {
	if env.profile == nil {
		return env, nil
	}
	phase := env.profile.start(env.profileParent, operation, detail)
	env.profileParent = phase.index + 1
	return env, phase
}

func (p *queryProfile) start(parent int, operation, detail string) *queryPhase {
	started := p.clock()
	key := queryPhaseKey{parent: parent, operation: operation, detail: detail}
	id, found := p.index[key]
	if !found {
		// IDs start at one; zero is the parent of the root execution span.
		id = len(p.nodes) + 1
		p.index[key] = id
		p.nodes = append(p.nodes, queryPhaseStats{queryPhaseKey: key})
	}
	phase := &queryPhase{profile: p, index: id - 1, started: started}
	if p.statementLifecycle {
		phase.previous = p.active
		p.active = phase
	}
	return phase
}

func (phase *queryPhase) finish(rows int64) {
	if phase == nil || phase.profile == nil {
		return
	}
	p := phase.profile
	ended := p.clock()
	// A panic can skip a child's explicit finish. Close any interrupted
	// spans at their enclosing boundary so a returned partial profile still
	// accounts for all elapsed time.
	if p.statementLifecycle {
		for p.active != phase {
			p.active.finishAt(ended, 0)
		}
	}
	phase.finishAt(ended, rows)
}

func (phase *queryPhase) finishAt(ended time.Time, rows int64) {
	p := phase.profile
	elapsed := ended.Sub(phase.started)
	node := &p.nodes[phase.index]
	node.calls++
	if rows >= 0 || !p.statementLifecycle {
		node.rows += rows
	}
	node.total += elapsed
	if node.parent != 0 {
		p.nodes[node.parent-1].children += elapsed
	}
	if p.statementLifecycle {
		p.active = phase.previous
	}
	phase.profile = nil
}

func (p *queryProfile) report() QueryProfile {
	report := QueryProfile{}
	if len(p.nodes) == 0 {
		return report
	}
	report.Elapsed = p.nodes[0].total
	report.Stages = make([]QueryStage, len(p.nodes))
	for i, node := range p.nodes {
		report.Stages[i] = QueryStage{
			Operation: node.operation, Detail: node.detail, Parent: node.parent,
			Calls: node.calls, Rows: node.rows, Total: node.total, Self: node.total - node.children,
		}
	}
	return report
}

func profileResultRows(result *ResultSet) int64 {
	if result == nil {
		return 0
	}
	return int64(len(result.Rows))
}

func (p *queryProfile) appendRows(rows *[]Row) {
	if len(p.nodes) == 0 {
		return
	}
	base := len(*rows) + 1
	total := p.nodes[0].total
	hottest := 0
	for i, node := range p.nodes {
		if node.total-node.children > p.nodes[hottest].total-p.nodes[hottest].children {
			hottest = i
		}
	}
	for i, node := range p.nodes {
		self := node.total - node.children
		percent := float64(0)
		if total > 0 {
			percent = float64(self) / float64(total) * 100
		}
		var parent any
		if node.parent != 0 {
			parent = base + node.parent - 1
		}
		*rows = append(*rows, Row{
			"operation": node.operation, "detail": node.detail,
			"parent_step": parent, "calls": node.calls, "actual_rows": node.rows,
			"total_ns": int64(node.total), "self_ns": int64(self),
			"percent": percent, "bottleneck": i == hottest,
		})
	}
}
