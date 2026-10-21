package engine

import "time"

// The executor runs a statement's stages synchronously. A collector belongs
// to one execution, never to a cached plan or AST. Repeated calls at the same
// path (for example recursive CTE iterations) accumulate instead of retaining a trace
// entry per input row.
type queryProfile struct {
	clock func() time.Time
	index map[queryPhaseKey]int
	nodes []queryPhaseStats
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
	profile *queryProfile
	index   int
	started time.Time
}

func newQueryProfile() *queryProfile {
	return &queryProfile{clock: time.Now, index: make(map[queryPhaseKey]int)}
}

func startQueryPhase(env ExecEnv, operation, detail string) (ExecEnv, *queryPhase) {
	if env.profile == nil {
		return env, nil
	}
	p := env.profile
	started := p.clock()
	key := queryPhaseKey{parent: env.profileParent, operation: operation, detail: detail}
	id, found := p.index[key]
	if !found {
		// IDs start at one; zero is the parent of the root execution span.
		id = len(p.nodes) + 1
		p.index[key] = id
		p.nodes = append(p.nodes, queryPhaseStats{queryPhaseKey: key})
	}
	env.profileParent = id
	return env, &queryPhase{profile: p, index: id - 1, started: started}
}

func (phase *queryPhase) finish(rows int64) {
	if phase == nil || phase.profile == nil {
		return
	}
	p := phase.profile
	elapsed := p.clock().Sub(phase.started)
	node := &p.nodes[phase.index]
	node.calls++
	node.rows += rows
	node.total += elapsed
	if node.parent != 0 {
		p.nodes[node.parent-1].children += elapsed
	}
	phase.profile = nil
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
