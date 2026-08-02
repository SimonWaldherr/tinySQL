//go:build sqliteimport && !js && !wasm && !baremetal

package main

// Renders one 256x256 PNG tile of a purely generative, non-geographic "world"
// used only to give the browser demo something worth panning around. The
// field is continuous across the whole [0,1)x[0,1) tile grid (see noise.go),
// so parent and child tiles at every zoom show the same underlying world at
// higher resolution, the way a real raster tile pyramid does.
//
// Elevation and moisture are quantized into a handful of bands before being
// turned into color (see quantize in noise.go), so each tile is made of a
// small number of flat-colored regions rather than a continuous gradient.
// That is both a deliberate "flat design" look and, because PNG compresses
// large uniform runs far better than fine-grained noise, the main reason 341
// tiles fit in a few hundred KB instead of several megabytes.

import (
	"bytes"
	"image"
	"image/color"
	"image/png"
)

const tileSize = 256

const (
	elevationLevels = 18
	moistureLevels  = 4
)

type colorStop struct {
	t       float64
	r, g, b uint8
}

// bands is an elevation gradient loosely modeled on topographic map styling:
// ocean depths through coast, grassland, forest, highland, and snow.
var bands = []colorStop{
	{0.00, 8, 28, 64},
	{0.34, 15, 58, 110},
	{0.40, 31, 112, 150},
	{0.455, 140, 196, 196},
	{0.475, 217, 201, 150},
	{0.50, 150, 189, 110},
	{0.66, 90, 150, 80},
	{0.76, 47, 97, 58},
	{0.85, 143, 122, 96},
	{0.92, 150, 145, 140},
	{0.965, 214, 214, 214},
	{1.01, 255, 255, 255},
}

func gradientColor(t float64) (float64, float64, float64) {
	if t <= bands[0].t {
		s := bands[0]
		return float64(s.r), float64(s.g), float64(s.b)
	}
	for i := 1; i < len(bands); i++ {
		if t <= bands[i].t {
			a, b := bands[i-1], bands[i]
			span := b.t - a.t
			f := 0.0
			if span > 0 {
				f = (t - a.t) / span
			}
			r := lerp(float64(a.r), float64(b.r), f)
			g := lerp(float64(a.g), float64(b.g), f)
			bl := lerp(float64(a.b), float64(b.b), f)
			return r, g, bl
		}
	}
	last := bands[len(bands)-1]
	return float64(last.r), float64(last.g), float64(last.b)
}

// moistureTint nudges the grassland band between an olive (dry) and a deep
// green (wet) hue so the field reads as biomes rather than a flat ramp.
func moistureTint(t, moist, r, g, b float64) (float64, float64, float64) {
	if t < 0.50 || t > 0.76 {
		return r, g, b
	}
	dryR, dryG, dryB := 180.0, 175.0, 90.0
	wetR, wetG, wetB := 40.0, 110.0, 60.0
	tintR := lerp(dryR, wetR, moist)
	tintG := lerp(dryG, wetG, moist)
	tintB := lerp(dryB, wetB, moist)
	const weight = 0.42
	return lerp(r, tintR, weight), lerp(g, tintG, weight), lerp(b, tintB, weight)
}

// tilePalette accumulates the small, shared set of flat colors every tile is
// built from, so the whole tileset (and each individual PNG) stays an
// indexed image instead of 24-bit RGB.
type tilePalette struct {
	index map[color.RGBA]uint8
	table color.Palette
}

func newTilePalette() *tilePalette {
	return &tilePalette{index: make(map[color.RGBA]uint8, 256)}
}

func (p *tilePalette) indexFor(c color.RGBA) uint8 {
	if i, ok := p.index[c]; ok {
		return i
	}
	if len(p.table) >= 256 {
		// Every color this generator can produce is enumerable at build time
		// (elevationLevels x moistureLevels x border shading), so running out
		// of palette slots means a constant above was widened without
		// widening this budget -- fail loudly rather than silently degrade.
		panic("mbtilesdemo: tile palette exceeded 256 colors")
	}
	i := uint8(len(p.table))
	p.table = append(p.table, c)
	p.index[c] = i
	return i
}

// renderTile paints one tile of the generative field at XYZ address (z,x,y).
func renderTile(z, x, y int, pal *tilePalette) *image.Paletted {
	n := float64(int(1) << uint(z))
	img := image.NewPaletted(image.Rect(0, 0, tileSize, tileSize), pal.table)
	for py := 0; py < tileSize; py++ {
		v := (float64(y) + (float64(py)+0.5)/tileSize) / n
		for px := 0; px < tileSize; px++ {
			u := (float64(x) + (float64(px)+0.5)/tileSize) / n

			elev := quantize(fbm(u, v, 1, 4, 3), elevationLevels)
			moist := quantize(fbm(u, v, 4201, 3, 2), moistureLevels)

			r, g, b := gradientColor(elev)
			r, g, b = moistureTint(elev, moist, r, g, b)

			c := color.RGBA{R: clamp8(r), G: clamp8(g), B: clamp8(b), A: 255}
			if isBorderPixel(px, py) {
				c = darken(c, 0.86)
			}
			img.SetColorIndex(px, py, pal.indexFor(c))
		}
	}
	img.Palette = pal.table
	return img
}

func isBorderPixel(x, y int) bool {
	return x == 0 || y == 0 || x == tileSize-1 || y == tileSize-1
}

// darken is used both while painting the border ring and to precompute the
// palette, so the border's darkened colors are counted up front.
func darken(c color.RGBA, shade float64) color.RGBA {
	return color.RGBA{
		R: uint8(float64(c.R) * shade),
		G: uint8(float64(c.G) * shade),
		B: uint8(float64(c.B) * shade),
		A: 255,
	}
}

func clamp8(v float64) uint8 {
	if v < 0 {
		return 0
	}
	if v > 255 {
		return 255
	}
	return uint8(v)
}

// buildPalette walks every reachable (elevation band, moisture band) pair --
// the entire color space this generator can produce, since both inputs are
// quantized before reaching gradientColor -- and registers each color (and
// its border-darkened variant) up front. Doing this once, before rendering
// any tile, means every tile shares one fixed-size global.Palette instead of
// each PNG carrying its own, which is both smaller on disk and the reason
// indexFor never needs to grow the table mid-render.
func buildPalette() *tilePalette {
	pal := newTilePalette()
	for ei := 0; ei < elevationLevels; ei++ {
		elev := float64(ei) / float64(elevationLevels-1)
		for mi := 0; mi < moistureLevels; mi++ {
			moist := float64(mi) / float64(moistureLevels-1)
			r, g, b := gradientColor(elev)
			r, g, b = moistureTint(elev, moist, r, g, b)
			c := color.RGBA{R: clamp8(r), G: clamp8(g), B: clamp8(b), A: 255}
			pal.indexFor(c)
			pal.indexFor(darken(c, 0.86))
		}
	}
	return pal
}

// encodePNG renders to compressed PNG bytes.
func encodePNG(img *image.Paletted) ([]byte, error) {
	var buf bytes.Buffer
	enc := png.Encoder{CompressionLevel: png.BestCompression}
	if err := enc.Encode(&buf, img); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
