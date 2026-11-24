package engine

import "sync"

type vecColumnShape struct {
	vectors, dims, distinctDims, excluded int
}

// A cache entry is copied by value and shared by concurrent readers. Keep
// lazy state behind one pointer rather than copying a sync.Once with it.
// Norm upgrades and unrelated column updates share this state. Its identity
// also identifies the vector values and row IDs, so any change to them needs
// fresh state even if it would produce the same counts and dimensions.
type vecColumnShapeCache struct {
	once  sync.Once
	value vecColumnShape
}

func (c vecSearchColumnCacheEntry) columnShape() vecColumnShape {
	if c.shape == nil {
		// Hand-built cache views remain valid without production cache state.
		return c.computeColumnShape()
	}
	c.shape.once.Do(func() { c.shape.value = c.computeColumnShape() })
	return c.shape.value
}

func (c vecSearchColumnCacheEntry) computeColumnShape() vecColumnShape {
	var shape vecColumnShape
	lengths := make(map[int]int)
	for row := 0; row < c.rowCount(); row++ {
		vector, _, valid := c.resolveRow(row, false)
		if !valid {
			continue
		}
		shape.vectors++
		length := len(vector)
		lengths[length]++
		// Empty vectors count as a distinct length, but the chosen dimension
		// is the first nonzero length, matching the original warm-up scan.
		if shape.dims == 0 {
			shape.dims = length
		}
	}
	shape.distinctDims = len(lengths)
	if shape.dims != 0 {
		shape.excluded = shape.vectors - lengths[shape.dims]
	}
	return shape
}
