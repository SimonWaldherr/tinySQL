package routing

import "math"

const earthRadius = 6371000.0

type vec3 struct{ x, y, z float64 }

func (a vec3) add(b vec3) vec3      { return vec3{a.x + b.x, a.y + b.y, a.z + b.z} }
func (a vec3) sub(b vec3) vec3      { return vec3{a.x - b.x, a.y - b.y, a.z - b.z} }
func (a vec3) scale(s float64) vec3 { return vec3{a.x * s, a.y * s, a.z * s} }
func (a vec3) dot(b vec3) float64   { return a.x*b.x + a.y*b.y + a.z*b.z }
func (a vec3) cross(b vec3) vec3 {
	return vec3{a.y*b.z - a.z*b.y, a.z*b.x - a.x*b.z, a.x*b.y - a.y*b.x}
}
func (a vec3) norm() float64 { return math.Sqrt(a.dot(a)) }
func (a vec3) unit() vec3    { return a.scale(1 / a.norm()) }
func vector(p Point) vec3 {
	lat, lon := p.Lat*math.Pi/180, p.Lon*math.Pi/180
	c := math.Cos(lat)
	return vec3{c * math.Cos(lon), c * math.Sin(lon), math.Sin(lat)}
}
func coordinate(v vec3) Point {
	return Point{math.Atan2(v.y, v.x) * 180 / math.Pi, math.Atan2(v.z, math.Hypot(v.x, v.y)) * 180 / math.Pi}
}
func angle(a, b vec3) float64     { return math.Atan2(a.cross(b).norm(), a.dot(b)) }
func distance(a, b Point) float64 { return angle(vector(a), vector(b)) * earthRadius }

// Project to the minor great-circle arc, not to longitude/latitude degrees.
func project(p, a, b vec3) (vec3, float64, float64) {
	theta := angle(a, b)
	best, f := a, 0.0
	d := angle(p, a)
	if v := angle(p, b); v < d {
		best, f, d = b, 1, v
	}
	n := a.cross(b)
	nn := n.dot(n)
	if nn > 1e-24 && theta < math.Pi-1e-10 {
		q := p.sub(n.scale(p.dot(n) / nn))
		if q.norm() > 1e-12 {
			q = q.unit()
			aq, qb := angle(a, q), angle(q, b)
			if aq+qb <= theta+1e-10 {
				if v := angle(p, q); v < d {
					best, f, d = q, math.Max(0, math.Min(1, aq/theta)), v
				}
			}
		}
	}
	return best, f, d * earthRadius
}

type box3 struct{ lo, hi vec3 }

func arcBox(a, b vec3) box3 {
	c := a.add(b).scale(.5)
	radius := a.sub(b).norm()*.5 + 1e-12
	d := vec3{radius, radius, radius}
	return box3{c.sub(d), c.add(d)}
}
func union(a, b box3) box3 {
	return box3{vec3{math.Min(a.lo.x, b.lo.x), math.Min(a.lo.y, b.lo.y), math.Min(a.lo.z, b.lo.z)}, vec3{math.Max(a.hi.x, b.hi.x), math.Max(a.hi.y, b.hi.y), math.Max(a.hi.z, b.hi.z)}}
}
func (b box3) squared(p vec3) float64 {
	d := func(v, lo, hi float64) float64 {
		if v < lo {
			return lo - v
		}
		if v > hi {
			return v - hi
		}
		return 0
	}
	x, y, z := d(p.x, b.lo.x, b.hi.x), d(p.y, b.lo.y, b.hi.y), d(p.z, b.lo.z, b.hi.z)
	return x*x + y*y + z*z
}
