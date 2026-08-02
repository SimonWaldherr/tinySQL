//go:build sqliteimport && !js && !wasm && !baremetal

package main

// A tiny deterministic value-noise field used to paint the demo tileset.
//
// The field is defined once in normalized world space [0,1)x[0,1) and sampled
// per tile-pixel, so every zoom level renders a consistent crop of the same
// continuous world instead of unrelated noise per tile -- the same property a
// real multi-resolution raster pyramid has.

import "math"

// hash mixes two grid coordinates into a value in [0,1). It is an integer
// bit-mixer (splitmix64-style), not a trig-based hash, so it stays well
// distributed at the coarse grid sizes this generator samples.
func hash(ix, iy int32, seed uint32) float64 {
	h := uint64(uint32(ix))*0x9E3779B1 + uint64(uint32(iy))*0x85EBCA77 + uint64(seed)*0xC2B2AE3D
	h ^= h >> 33
	h *= 0xFF51AFD7ED558CCD
	h ^= h >> 33
	h *= 0xC4CEB9FE1A85EC53
	h ^= h >> 33
	return float64(h&0xFFFFFFFF) / float64(0xFFFFFFFF)
}

func smootherstep(t float64) float64 {
	return t * t * t * (t*(t*6-15) + 10)
}

func lerp(a, b, t float64) float64 { return a + (b-a)*t }

// valueNoise2D samples a single octave of value noise at frequency cells
// across the [0,1) torus (wrapping so u=0 and u=1 line up seamlessly, which
// matters because tile x=0 and the antimeridian tile sit next to each other).
func valueNoise2D(u, v float64, cells int, seed uint32) float64 {
	fx := u * float64(cells)
	fy := v * float64(cells)
	x0 := int32(math.Floor(fx))
	y0 := int32(math.Floor(fy))
	tx := smootherstep(fx - math.Floor(fx))
	ty := smootherstep(fy - math.Floor(fy))
	wrap := func(i int32) int32 {
		m := i % int32(cells)
		if m < 0 {
			m += int32(cells)
		}
		return m
	}
	x1, y1 := wrap(x0+1), wrap(y0+1)
	x0w, y0w := wrap(x0), wrap(y0)
	v00 := hash(x0w, y0w, seed)
	v10 := hash(x1, y0w, seed)
	v01 := hash(x0w, y1, seed)
	v11 := hash(x1, y1, seed)
	top := lerp(v00, v10, tx)
	bot := lerp(v01, v11, tx)
	return lerp(top, bot, ty)
}

// fbm combines octaves of valueNoise2D into a smoother field in [0,1].
// startCells and octaves are deliberately small for this demo: fewer, larger
// features posterize into big flat-colored regions once quantized (see
// quantize in tileimage.go), which is both a nicer "flat design" look and,
// because PNG compresses large uniform runs far better than fine noise, most
// of what keeps the generated tileset small enough to ship on gh-pages.
func fbm(u, v float64, seed uint32, startCells, octaves int) float64 {
	sum, amp, total := 0.0, 1.0, 0.0
	cells := startCells
	for octave := 0; octave < octaves; octave++ {
		sum += valueNoise2D(u, v, cells, seed+uint32(octave)*97) * amp
		total += amp
		amp *= 0.52
		cells *= 2
	}
	return sum / total
}

// quantize snaps t into one of levels evenly spaced steps across [0,1],
// turning a continuous field into flat bands.
func quantize(t float64, levels int) float64 {
	if levels <= 1 {
		return t
	}
	step := math.Round(t * float64(levels-1))
	return step / float64(levels-1)
}
