package routing

import (
	"context"
	"fmt"
	"math"
)

// Build compiles one immutable routing profile. Unsupported conditional rules
// return an error; no route is returned under silently discarded restrictions.
func Build(ctx context.Context, data Data, p Profile, opts Options) (*Router, error) {
	if p != Car && p != Bicycle && p != Foot {
		return nil, fmt.Errorf("routing: unknown profile %q", p)
	}
	if opts.MaxStates < 0 || opts.MaxRestrictionPrefixes < 0 {
		return nil, fmt.Errorf("routing: negative resource limit")
	}
	if opts.MaxStates == 0 {
		opts.MaxStates = 1000000
	}
	if opts.MaxRestrictionPrefixes == 0 {
		opts.MaxRestrictionPrefixes = 1000000
	}
	r := &Router{profile: p, maxStates: opts.MaxStates}
	nodeIndex := make(map[int64]int, len(data.Nodes))
	allowed := make([]bool, len(data.Nodes))
	for i, n := range data.Nodes {
		if i&1023 == 0 {
			if err := check(ctx); err != nil {
				return nil, err
			}
		}
		if _, ok := nodeIndex[n.ID]; ok {
			return nil, fmt.Errorf("routing: duplicate node %d", n.ID)
		}
		if err := n.Point.validate(); err != nil {
			return nil, fmt.Errorf("node %d: %w", n.ID, err)
		}
		nodeIndex[n.ID] = i
		r.nodes = append(r.nodes, node{n.ID, n.Point, vector(n.Point)})
		var err error
		allowed[i], err = nodeAllowed(n, p)
		if err != nil {
			return nil, err
		}
	}
	ways := make(map[int64]Way, len(data.Ways))
	wayEdges := make(map[int64][]int)
	for wi, w := range data.Ways {
		if wi&255 == 0 {
			if err := check(ctx); err != nil {
				return nil, err
			}
		}
		if _, exists := ways[w.ID]; exists {
			return nil, fmt.Errorf("routing: duplicate way %d", w.ID)
		}
		ways[w.ID] = w
		f, b, err := waySpeeds(w, p)
		if err != nil {
			return nil, err
		}
		if f == 0 && b == 0 {
			continue
		}
		for j := 1; j < len(w.Nodes); j++ {
			a, okA := nodeIndex[w.Nodes[j-1]]
			z, okB := nodeIndex[w.Nodes[j]]
			if !okA || !okB {
				return nil, fmt.Errorf("way %d: missing referenced node", w.ID)
			}
			if a == z || !allowed[a] || !allowed[z] {
				continue
			}
			meters := angle(r.nodes[a].xyz, r.nodes[z].xyz) * earthRadius
			if meters <= 1e-8 {
				continue
			}
			if meters > math.Pi*earthRadius-1 {
				return nil, fmt.Errorf("way %d: ambiguous antipodal segment", w.ID)
			}
			sid := len(r.segments)
			seg := segment{a: a, b: z, way: w.ID, forward: -1, backward: -1, meters: meters, box: arcBox(r.nodes[a].xyz, r.nodes[z].xyz)}
			if f > 0 {
				seg.forward = len(r.edges)
				wayEdges[w.ID] = append(wayEdges[w.ID], len(r.edges))
				r.edges = append(r.edges, edge{a, z, sid, meters / f, meters})
				r.maxSpeed = math.Max(r.maxSpeed, f)
			}
			if b > 0 {
				seg.backward = len(r.edges)
				wayEdges[w.ID] = append(wayEdges[w.ID], len(r.edges))
				r.edges = append(r.edges, edge{z, a, sid, meters / b, meters})
				r.maxSpeed = math.Max(r.maxSpeed, b)
			}
			r.segments = append(r.segments, seg)
		}
	}
	// Keep only routable nodes: building/POI nodes must not inflate per-query scratch.
	remap := make([]int, len(r.nodes))
	for i := range remap {
		remap[i] = -1
	}
	for _, e := range r.edges {
		remap[e.from] = 0
		remap[e.to] = 0
	}
	compact := make([]node, 0)
	for i, n := range r.nodes {
		if remap[i] == 0 {
			remap[i] = len(compact)
			compact = append(compact, n)
		}
		nodeIndex[n.id] = remap[i]
	}
	for i := range r.edges {
		e := &r.edges[i]
		e.from = remap[e.from]
		e.to = remap[e.to]
	}
	for i := range r.segments {
		s := &r.segments[i]
		s.a = remap[s.a]
		s.b = remap[s.b]
	}
	r.nodes = compact
	r.offsets = make([]int, len(r.nodes)+1)
	for _, e := range r.edges {
		r.offsets[e.from+1]++
	}
	for i := 1; i < len(r.offsets); i++ {
		r.offsets[i] += r.offsets[i-1]
	}
	r.adj = make([]int, len(r.edges))
	cursor := append([]int(nil), r.offsets...)
	for i, e := range r.edges {
		r.adj[cursor[e.from]] = i
		cursor[e.from]++
	}
	if err := r.buildRestrictions(ctx, data.Relations, ways, wayEdges, nodeIndex, opts.MaxRestrictionPrefixes); err != nil {
		return nil, err
	}
	if err := check(ctx); err != nil {
		return nil, err
	}
	r.buildSpatial()
	if err := check(ctx); err != nil {
		return nil, err
	}
	return r, nil
}
