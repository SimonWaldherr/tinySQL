package routing

import (
	"context"
	"math"
	"sort"
)

type bvhNode struct {
	box                     box3
	left, right, start, end int
}

func (r *Router) buildSpatial() {
	r.spatialOrder = make([]int, len(r.segments))
	for i := range r.spatialOrder {
		r.spatialOrder[i] = i
	}
	var build func(int, int) int
	build = func(start, end int) int {
		bounds := r.segments[r.spatialOrder[start]].box
		for _, id := range r.spatialOrder[start+1 : end] {
			bounds = union(bounds, r.segments[id].box)
		}
		index := len(r.spatial)
		r.spatial = append(r.spatial, bvhNode{box: bounds, left: -1, right: -1, start: start, end: end})
		if end-start <= 8 {
			return index
		}
		extent := bounds.hi.sub(bounds.lo)
		axis := 0
		if extent.y > extent.x {
			axis = 1
		}
		if extent.z > math.Max(extent.x, extent.y) {
			axis = 2
		}
		center := func(id int) float64 {
			b := r.segments[id].box
			switch axis {
			case 0:
				return b.lo.x + b.hi.x
			case 1:
				return b.lo.y + b.hi.y
			default:
				return b.lo.z + b.hi.z
			}
		}
		order := r.spatialOrder[start:end]
		sort.Slice(order, func(i, j int) bool {
			a, b := center(order[i]), center(order[j])
			if a == b {
				return order[i] < order[j]
			}
			return a < b
		})
		mid := (start + end) / 2
		left, right := build(start, mid), build(mid, end)
		r.spatial[index].left, r.spatial[index].right = left, right
		return index
	}
	if len(r.segments) > 0 {
		build(0, len(r.segments))
	}
}

// Nearest returns the exact nearest eligible road segment within maxMeters.
// The BVH uses conservative 3-D bounds, including at the poles/date line.
func (r *Router) Nearest(ctx context.Context, p Point, maxMeters float64) (Snap, error) {
	if err := p.validate(); err != nil {
		return Snap{}, err
	}
	if err := validRadius(maxMeters); err != nil {
		return Snap{}, err
	}
	if err := check(ctx); err != nil {
		return Snap{}, err
	}
	v := vector(p)
	best := Snap{DistanceMeters: maxMeters, segment: -1}
	visits := 0
	var walk func(int) error
	walk = func(index int) error {
		visits++
		if visits&255 == 0 {
			if err := check(ctx); err != nil {
				return err
			}
		}
		b := r.spatial[index]
		chord := 2 * math.Sin(best.DistanceMeters/earthRadius/2)
		if b.box.squared(v) > chord*chord+1e-20 {
			return nil
		}
		if b.left < 0 {
			for _, id := range r.spatialOrder[b.start:b.end] {
				s := r.segments[id]
				q, f, d := project(v, r.nodes[s.a].xyz, r.nodes[s.b].xyz)
				if d < best.DistanceMeters || (d == best.DistanceMeters && (best.segment < 0 || id < best.segment)) {
					best = Snap{coordinate(q), d, s.way, f, id}
				}
			}
			return nil
		}
		left, right := b.left, b.right
		if r.spatial[right].box.squared(v) < r.spatial[left].box.squared(v) {
			left, right = right, left
		}
		if err := walk(left); err != nil {
			return err
		}
		return walk(right)
	}
	if len(r.spatial) > 0 {
		if err := walk(0); err != nil {
			return Snap{}, err
		}
	}
	if best.segment < 0 {
		return Snap{}, ErrNoSnap
	}
	return best, nil
}
