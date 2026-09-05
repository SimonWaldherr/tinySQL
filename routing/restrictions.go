package routing

import (
	"context"
	"fmt"
	"strings"
)

type ruleNode struct {
	next                   map[int]int
	fail                   int
	banned, only, onlyDone bool
	allowed                map[int]bool
}
type automaton struct {
	nodes []ruleNode
	limit int
}

func (a *automaton) prefix(edges []int) (int, error) {
	at := 0
	for _, edge := range edges {
		next, ok := a.nodes[at].next[edge]
		if !ok {
			if len(a.nodes) >= a.limit {
				return 0, fmt.Errorf("routing: restriction prefix limit exceeded")
			}
			next = len(a.nodes)
			a.nodes = append(a.nodes, ruleNode{})
			if a.nodes[at].next == nil {
				a.nodes[at].next = make(map[int]int)
			}
			a.nodes[at].next[edge] = next
		}
		at = next
	}
	return at, nil
}
func (a *automaton) add(edges []int, only bool) error {
	if _, err := a.prefix(edges); err != nil {
		return err
	}
	at := 0
	for i := 1; i <= len(edges); i++ {
		at = a.nodes[at].next[edges[i-1]]
		if only {
			if i < len(edges) {
				a.nodes[at].only = true
				if a.nodes[at].allowed == nil {
					a.nodes[at].allowed = make(map[int]bool)
				}
				a.nodes[at].allowed[edges[i]] = true
			} else {
				a.nodes[at].onlyDone = true
			}
		} else if i == len(edges) {
			a.nodes[at].banned = true
		}
	}
	return nil
}
func (a *automaton) gate(edge int) error {
	at, err := a.prefix([]int{edge})
	if err != nil {
		return err
	}
	a.nodes[at].only = true
	return nil
}
func (a *automaton) finish() {
	queue := make([]int, 0, len(a.nodes))
	for _, child := range a.nodes[0].next {
		queue = append(queue, child)
	}
	for head := 0; head < len(queue); head++ {
		at := queue[head]
		for edge, child := range a.nodes[at].next {
			f := a.nodes[at].fail
			for f != 0 {
				if _, ok := a.nodes[f].next[edge]; ok {
					break
				}
				f = a.nodes[f].fail
			}
			if next, ok := a.nodes[f].next[edge]; ok {
				a.nodes[child].fail = next
			}
			queue = append(queue, child)
		}
	}
}

// step carries the relevant restriction history through the search state.
// Failure links retain overlapping prefixes, so a route cannot evade a rule
// by passing through a second restricted intersection along its via ways.
func (a *automaton) step(state, edge int) (int, bool) {
	for at := state; at != 0; at = a.nodes[at].fail {
		n := &a.nodes[at]
		if n.only && !n.onlyDone && !n.allowed[edge] {
			return 0, false
		}
	}
	at := state
	for at != 0 {
		if _, ok := a.nodes[at].next[edge]; ok {
			break
		}
		at = a.nodes[at].fail
	}
	next := a.nodes[at].next[edge]
	for at := next; at != 0; at = a.nodes[at].fail {
		if a.nodes[at].banned {
			return 0, false
		}
	}
	return next, true
}
func restrictionValue(rel Relation, p Profile) (string, error) {
	modes := []string{"vehicle", "motor_vehicle", "motorcar"}
	if p == Bicycle {
		modes = []string{"vehicle", "bicycle"}
	}
	if p == Foot {
		modes = []string{"foot"}
	}
	for _, except := range strings.Split(rel.Tags["except"], ";") {
		except = strings.TrimSpace(except)
		for _, mode := range modes {
			if except == mode {
				return "", nil
			}
		}
	}
	value := ""
	if p != Foot {
		value = rel.Tags["restriction"]
		if rel.Tags["restriction:conditional"] != "" {
			return "", fmt.Errorf("relation %d: conditional restriction unsupported", rel.ID)
		}
	}
	for _, mode := range modes {
		if v := rel.Tags["restriction:"+mode]; v != "" {
			value = v
		}
		if rel.Tags["restriction:"+mode+":conditional"] != "" {
			return "", fmt.Errorf("relation %d: conditional restriction unsupported", rel.ID)
		}
	}
	switch value {
	case "", "no_left_turn", "no_right_turn", "no_straight_on", "no_u_turn", "no_entry", "no_exit", "only_right_turn", "only_left_turn", "only_straight_on", "only_u_turn":
	default:
		return "", fmt.Errorf("relation %d: unsupported restriction %q", rel.ID, value)
	}

	return value, nil
}
func (r *Router) buildRestrictions(ctx context.Context, relations []Relation, ways map[int64]Way, wayEdges map[int64][]int, nodeIndex map[int64]int, limit int) error {
	r.rules = automaton{nodes: []ruleNode{{}}, limit: limit}
	seen := make(map[int64]bool)
	for _, rel := range relations {
		if err := check(ctx); err != nil {
			return err
		}
		if seen[rel.ID] {
			return fmt.Errorf("routing: duplicate relation %d", rel.ID)
		}
		seen[rel.ID] = true
		if rel.Tags["type"] != "restriction" {
			continue
		}
		value, err := restrictionValue(rel, r.profile)
		if err != nil {
			return err
		}
		if value == "" {
			continue
		}
		only := strings.HasPrefix(value, "only_")
		var from, to []int64
		var via []Member
		for _, m := range rel.Members {
			switch m.Role {
			case "from", "to":
				if m.Type != "way" {
					return fmt.Errorf("relation %d: from/to must be ways", rel.ID)
				}
				if _, ok := ways[m.Ref]; !ok {
					return fmt.Errorf("relation %d: missing way %d", rel.ID, m.Ref)
				}
				if m.Role == "from" {
					from = append(from, m.Ref)
				} else {
					to = append(to, m.Ref)
				}
			case "via":
				via = append(via, m)
			}
		}
		if len(from) == 0 || len(to) == 0 || len(via) == 0 {
			return fmt.Errorf("relation %d: incomplete restriction", rel.ID)
		}
		if len(via) == 1 && via[0].Type == "node" {
			v, exists := nodeIndex[via[0].Ref]
			if !exists {
				return fmt.Errorf("relation %d: missing via node", rel.ID)
			}
			for _, fw := range from {
				for _, incoming := range wayEdges[fw] {
					if r.edges[incoming].to != v {
						continue
					}
					if only {
						if err := r.rules.gate(incoming); err != nil {
							return err
						}
					}
					for _, tw := range to {
						for _, outgoing := range wayEdges[tw] {
							e := r.edges[outgoing]
							if e.from != v {
								continue
							}
							reverse := e.segment == r.edges[incoming].segment && e.to == r.edges[incoming].from
							if value == "no_u_turn" || value == "only_u_turn" {
								if fw == tw && !reverse {
									continue
								}
							} else if value == "only_straight_on" && fw == tw && reverse {
								continue
							}
							if err := r.rules.add([]int{incoming, outgoing}, only); err != nil {
								return err
							}
						}
					}
				}
			}
			continue
		}
		viaIDs := make([]int64, len(via))
		for i, m := range via {
			if m.Type != "way" {
				return fmt.Errorf("relation %d: mixed/multiple via-node restriction unsupported", rel.ID)
			}
			if _, ok := ways[m.Ref]; !ok {
				return fmt.Errorf("relation %d: missing via way", rel.ID)
			}
			viaIDs[i] = m.Ref
		}
		// Enumerate legal directed traversals of each via way. Every intermediate
		// shape node is retained; matching just way IDs would conflate reversals.
		operations := 0
		var enter func(int, int, []int) error
		enter = func(stage, node int, path []int) error {
			operations++
			if operations > limit {
				return fmt.Errorf("relation %d: traversal limit exceeded", rel.ID)
			}
			if operations&255 == 0 {
				if err := check(ctx); err != nil {
					return err
				}
			}
			if stage == len(viaIDs) {
				for _, tw := range to {
					for _, out := range wayEdges[tw] {
						if r.edges[out].from == node {
							sequence := append(append([]int(nil), path...), out)
							if err := r.rules.add(sequence, only); err != nil {
								return err
							}
						}
					}
				}
				return nil
			}
			visited := map[int]bool{node: true}
			var walk func(int, []int) error
			walk = func(current int, prefix []int) error {
				operations++
				if operations > limit || len(prefix) > 4096 {
					return fmt.Errorf("relation %d: traversal limit exceeded", rel.ID)
				}
				if operations&255 == 0 {
					if err := check(ctx); err != nil {
						return err
					}
				}
				for _, id := range r.adj[r.offsets[current]:r.offsets[current+1]] {
					e := r.edges[id]
					if r.segments[e.segment].way != viaIDs[stage] || visited[e.to] {
						continue
					}
					visited[e.to] = true
					next := append(prefix, id)
					if err := enter(stage+1, e.to, next); err != nil {
						return err
					}
					if err := walk(e.to, next); err != nil {
						return err
					}
					delete(visited, e.to)
				}
				return nil
			}
			return walk(node, path)
		}
		for _, fw := range from {
			for _, incoming := range wayEdges[fw] {
				at := r.edges[incoming].to
				canEnter := false
				// An inaccessible via way still constrains an only_* approach.
				for _, nodeID := range ways[viaIDs[0]].Nodes {
					if nodeID == r.nodes[at].id {
						canEnter = true
						break
					}
				}
				if !canEnter {
					continue
				}
				if only {
					if err := r.rules.gate(incoming); err != nil {
						return err
					}
				}
				if err := enter(0, at, []int{incoming}); err != nil {
					return err
				}
			}
		}
	}
	r.rules.finish()
	return nil
}
