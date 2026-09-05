package routing

import (
	"context"
	"fmt"
	"math"
)

func validRadius(radius float64) error {
	if math.IsNaN(radius) || math.IsInf(radius, 0) || radius <= 0 || radius > 5000 {
		return fmt.Errorf("routing: max_snap_meters must be in (0,5000]")
	}
	return nil
}

type queueEntry struct {
	record         int
	cost, priority float64
}
type queue []queueEntry

func less(a, b queueEntry) bool {
	if a.priority == b.priority {
		return a.record < b.record
	}
	return a.priority < b.priority
}
func (q *queue) push(e queueEntry) {
	*q = append(*q, e)
	i := len(*q) - 1
	for i > 0 {
		p := (i - 1) / 2
		if !less((*q)[i], (*q)[p]) {
			break
		}
		(*q)[i], (*q)[p] = (*q)[p], (*q)[i]
		i = p
	}
}
func (q *queue) pop() queueEntry {
	out := (*q)[0]
	last := len(*q) - 1
	(*q)[0] = (*q)[last]
	*q = (*q)[:last]
	for i := 0; ; {
		child := i*2 + 1
		if child >= last {
			break
		}
		if child+1 < last && less((*q)[child+1], (*q)[child]) {
			child++
		}
		if !less((*q)[child], (*q)[i]) {
			break
		}
		(*q)[child], (*q)[i] = (*q)[i], (*q)[child]
		i = child
	}
	return out
}

type record struct {
	node, rule, parent, via int
	cost, meters            float64
}
type stateKey struct{ node, rule int }
type partial struct {
	node, edge   int
	cost, meters float64
}

// Route computes a fastest legal route, including partial first/last segments.
// No prior answer, snapping result, or query scratch buffer is reused.
func (r *Router) Route(ctx context.Context, req Request) (Result, error) {
	return r.route(ctx, req, true)
}
func (r *Router) route(ctx context.Context, req Request, astar bool) (Result, error) {
	if req.MaxSnapMeters == 0 {
		req.MaxSnapMeters = 100
	}
	if err := validRadius(req.MaxSnapMeters); err != nil {
		return Result{}, err
	}
	from, err := r.Nearest(ctx, req.From, req.MaxSnapMeters)
	if err != nil {
		return Result{}, err
	}
	to, err := r.Nearest(ctx, req.To, req.MaxSnapMeters)
	if err != nil {
		return Result{}, err
	}
	startSeg, endSeg := r.segments[from.segment], r.segments[to.segment]
	endpoint := func(s segment, f float64) int {
		if f < 1e-10 {
			return s.a
		}
		if f > 1-1e-10 {
			return s.b
		}
		return -1
	}
	startNode, endNode := endpoint(startSeg, from.Fraction), endpoint(endSeg, to.Fraction)
	best, bestMeters, bestRecord := math.Inf(1), 0.0, -1
	if from.segment == to.segment {
		delta := to.Fraction - from.Fraction
		id := startSeg.forward
		if delta < 0 {
			id = startSeg.backward
		}
		if id >= 0 {
			best = math.Abs(delta) * r.edges[id].seconds
			bestMeters = math.Abs(delta) * startSeg.meters
		}
	}
	records := make([]record, 0, min(len(r.nodes), 1024))
	dense := make([]int, len(r.nodes))
	sparse := make(map[stateKey]int)
	heuristics := make([]float64, len(r.nodes))
	heap := make(queue, 0, 256)
	goalXYZ := vector(to.Point)
	heuristic := func(node int) float64 {
		if !astar {
			return 0
		}
		if heuristics[node] == 0 {
			heuristics[node] = angle(r.nodes[node].xyz, goalXYZ)*earthRadius/r.maxSpeed + 1
		}
		return heuristics[node] - 1
	}
	add := func(node, rule, parent, via int, cost, meters float64) error {
		if cost >= best {
			return nil
		}
		id := 0
		if rule == 0 {
			id = dense[node]
		} else {
			id = sparse[stateKey{node, rule}]
		}
		if id == 0 {
			if len(records) >= r.maxStates {
				return ErrSearchLimit
			}
			records = append(records, record{node, rule, parent, via, cost, meters})
			id = len(records)
			if rule == 0 {
				dense[node] = id
			} else {
				sparse[stateKey{node, rule}] = id
			}
		} else {
			if records[id-1].cost <= cost {
				return nil
			}
			records[id-1] = record{node, rule, parent, via, cost, meters}
		}
		heap.push(queueEntry{id - 1, cost, cost + heuristic(node)})
		return nil
	}
	if startNode >= 0 {
		if startNode == endNode {
			best, bestMeters = 0, 0
		} else if err := add(startNode, 0, -1, -1, 0, 0); err != nil {
			return Result{}, err
		}
	} else {
		for _, p := range []partial{{startSeg.b, startSeg.forward, 1 - from.Fraction, 0}, {startSeg.a, startSeg.backward, from.Fraction, 0}} {
			if p.edge < 0 {
				continue
			}
			rule, ok := r.rules.step(0, p.edge)
			if !ok {
				continue
			}
			e := r.edges[p.edge]
			if err := add(p.node, rule, -1, p.edge, p.cost*e.seconds, p.cost*e.meters); err != nil {
				return Result{}, err
			}
		}
	}
	var goals []partial
	if endNode < 0 {
		for _, p := range []partial{{endSeg.a, endSeg.forward, to.Fraction, 0}, {endSeg.b, endSeg.backward, 1 - to.Fraction, 0}} {
			if p.edge >= 0 {
				e := r.edges[p.edge]
				goals = append(goals, partial{p.node, p.edge, p.cost * e.seconds, p.cost * e.meters})
			}
		}
	}
	visited, pops := 0, 0
	for len(heap) > 0 {
		current := heap.pop()
		pops++
		if pops&255 == 1 {
			if err := check(ctx); err != nil {
				return Result{}, err
			}
		}
		rec := records[current.record]
		if current.cost != rec.cost {
			continue
		}
		if current.priority >= best {
			break
		}
		visited++
		if rec.node == endNode {
			best, bestMeters, bestRecord = rec.cost, rec.meters, current.record
			continue
		}
		for _, goal := range goals {
			if rec.node == goal.node {
				if _, ok := r.rules.step(rec.rule, goal.edge); ok && rec.cost+goal.cost < best {
					best, bestMeters, bestRecord = rec.cost+goal.cost, rec.meters+goal.meters, current.record
				}
			}
		}
		for _, edgeID := range r.adj[r.offsets[rec.node]:r.offsets[rec.node+1]] {
			e := r.edges[edgeID]
			rule, ok := r.rules.step(rec.rule, edgeID)
			if !ok {
				continue
			}
			if err := add(e.to, rule, current.record, edgeID, rec.cost+e.seconds, rec.meters+e.meters); err != nil {
				return Result{}, err
			}
		}
	}
	if math.IsInf(best, 1) {
		return Result{}, ErrNoRoute
	}
	if err := check(ctx); err != nil {
		return Result{}, err
	}
	result := Result{Profile: r.profile, DistanceMeters: bestMeters, DurationSeconds: best, From: from, To: to, VisitedStates: visited, Geometry: Geometry{Type: "LineString"}}
	path := make([]int, 0)
	for at := bestRecord; at >= 0; at = records[at].parent {
		path = append(path, at)
	}
	result.Geometry.Coordinates = append(result.Geometry.Coordinates, [2]float64{from.Point.Lon, from.Point.Lat})
	for i := len(path) - 1; i >= 0; i-- {
		p := r.nodes[records[path[i]].node].point
		result.Geometry.Coordinates = append(result.Geometry.Coordinates, [2]float64{p.Lon, p.Lat})
	}
	result.Geometry.Coordinates = append(result.Geometry.Coordinates, [2]float64{to.Point.Lon, to.Point.Lat})
	return result, nil
}
