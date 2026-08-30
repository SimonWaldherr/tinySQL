package engine

// TILE_COVER converts a WGS84 viewport into the minimal WebMercatorQuad/XYZ
// tile set that covers it. This is the bridge commonly needed by DTK/DOP
// downloaders, WMS-to-WMTS caches, offline navigation, and GIS export jobs.

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
)

const tileCoverDefaultMaxTiles = 65536

type TileCoverTableFunc struct{}

func (f *TileCoverTableFunc) Name() string { return "TILE_COVER" }

func (f *TileCoverTableFunc) ValidateArgs(args []Expr) error {
	if len(args) < 5 || len(args) > 6 {
		return fmt.Errorf("TILE_COVER requires (west, south, east, north, zoom [, max_tiles])")
	}
	return nil
}

type tileColumnRange struct{ min, max int }

func (f *TileCoverTableFunc) Execute(ctx context.Context, args []Expr, env ExecEnv, row Row) (*ResultSet, error) {
	if err := f.ValidateArgs(args); err != nil {
		return nil, err
	}
	if ctx == nil {
		ctx = env.ctx
	}
	if ctx == nil {
		ctx = context.Background()
	}
	coords := [4]float64{}
	for i := range coords {
		value, err := evalExpr(env, args[i], row)
		if err != nil {
			return nil, fmt.Errorf("TILE_COVER arg%d: %w", i+1, err)
		}
		coords[i], err = geoFloat(value)
		if err != nil || math.IsNaN(coords[i]) || math.IsInf(coords[i], 0) {
			if err == nil {
				err = fmt.Errorf("must be finite")
			}
			return nil, fmt.Errorf("TILE_COVER arg%d: %w", i+1, err)
		}
	}
	west, south, east, north := coords[0], coords[1], coords[2], coords[3]
	if west < -180 || west > 180 || east < -180 || east > 180 {
		return nil, fmt.Errorf("TILE_COVER: longitude bounds must be in -180..180")
	}
	if south < -90 || south > 90 || north < -90 || north > 90 || south > north {
		return nil, fmt.Errorf("TILE_COVER: expected -90 <= south <= north <= 90")
	}
	zoomValue, err := evalExpr(env, args[4], row)
	if err != nil {
		return nil, fmt.Errorf("TILE_COVER zoom: %w", err)
	}
	zoom, err := toInt(zoomValue)
	if err != nil || zoom < 0 || zoom > tileMaxZoom {
		if err == nil {
			err = fmt.Errorf("must be in range 0..%d", tileMaxZoom)
		}
		return nil, fmt.Errorf("TILE_COVER zoom: %w", err)
	}
	maxTiles := tileCoverDefaultMaxTiles
	if len(args) == 6 {
		value, err := evalExpr(env, args[5], row)
		if err != nil {
			return nil, fmt.Errorf("TILE_COVER max_tiles: %w", err)
		}
		maxTiles, err = toInt(value)
		if err != nil || maxTiles <= 0 || maxTiles > 1000000 {
			if err == nil {
				err = fmt.Errorf("must be in range 1..1000000")
			}
			return nil, fmt.Errorf("TILE_COVER max_tiles: %w", err)
		}
	}

	columnRanges := tileCoverColumnRanges(west, east, zoom)
	yMin := tileRowFor(north, zoom)
	yMax := yMin
	if south < north {
		yMax = tileCoverExclusiveSouthRow(south, zoom)
	}
	var count int64
	for _, xRange := range columnRanges {
		count += int64(xRange.max-xRange.min+1) * int64(yMax-yMin+1)
	}
	if count > int64(maxTiles) {
		return nil, fmt.Errorf("TILE_COVER: viewport requires %d tiles, exceeds max_tiles %d", count, maxTiles)
	}

	cols := []string{"z", "x", "y", "tile_row", "west", "south", "east", "north", "bbox_3857", "resolution", "scale_denominator", "quadkey"}
	rows := make([]Row, 0, int(count))
	extent := math.Pi * tileWebMercatorRadiusMeters
	projectedSpan := (2 * extent) / math.Ldexp(1, zoom)
	resolution := (2 * math.Pi * tileWebMercatorRadiusMeters) / (tileDefaultPixelSize * math.Ldexp(1, zoom))
	for _, xRange := range columnRanges {
		for x := xRange.min; x <= xRange.max; x++ {
			for y := yMin; y <= yMax; y++ {
				if len(rows)&1023 == 0 {
					if err := checkCtx(ctx); err != nil {
						return nil, err
					}
				}
				minX := -extent + float64(x)*projectedSpan
				maxY := extent - float64(y)*projectedSpan
				projected, err := json.Marshal([]float64{minX, maxY - projectedSpan, minX + projectedSpan, maxY})
				if err != nil {
					return nil, err
				}
				rows = append(rows, Row{
					"z": zoom, "x": x, "y": y, "tile_row": (1 << uint(zoom)) - 1 - y,
					"west": tileWestLon(x, zoom), "south": tileNorthLat(y+1, zoom),
					"east": tileWestLon(x+1, zoom), "north": tileNorthLat(y, zoom),
					"bbox_3857": string(projected), "resolution": resolution,
					"scale_denominator": resolution / wmtsStandardPixelMeters,
					"quadkey":           tileQuadkeyFor(zoom, x, y),
				})
			}
		}
	}
	return &ResultSet{Cols: cols, Rows: rows}, nil
}

// tileCoverColumnRanges uses half-open east bounds, matching XYZ tile extents,
// while a west>east viewport explicitly crosses the antimeridian.
func tileCoverColumnRanges(west, east float64, zoom int) []tileColumnRange {
	side := 1 << uint(zoom)
	if west == -180 && east == 180 {
		return []tileColumnRange{{min: 0, max: side - 1}}
	}
	if west == 180 && east == -180 {
		return []tileColumnRange{{min: 0, max: 0}}
	}
	xWest := tileColumnFor(west, zoom)
	xEast := xWest
	if east != west {
		// ceil()-1 implements an exclusive east edge without relying on a
		// Nextafter perturbation that can disappear when 180 is added.
		xEast = int(math.Ceil((east+180)/360*float64(side))) - 1
		if xEast < 0 {
			xEast = 0
		}
		if xEast >= side {
			xEast = side - 1
		}
	}
	if west <= east {
		return []tileColumnRange{{min: xWest, max: xEast}}
	}
	ranges := []tileColumnRange{{min: xWest, max: side - 1}}
	if xEast >= 0 {
		ranges = append(ranges, tileColumnRange{min: 0, max: xEast})
	}
	if len(ranges) == 2 && ranges[0].min == 0 && ranges[0].max == ranges[1].max {
		return ranges[:1]
	}
	return ranges
}

func tileCoverExclusiveSouthRow(lat float64, zoom int) int {
	rad := tileClampLat(lat) * math.Pi / 180
	frac := (1 - math.Log(math.Tan(rad)+1/math.Cos(rad))/math.Pi) / 2
	y := int(math.Ceil(frac*math.Ldexp(1, zoom))) - 1
	if y < 0 {
		return 0
	}
	side := (1 << uint(zoom)) - 1
	if y > side {
		return side
	}
	return y
}

func init() {
	RegisterTableFunc(&TileCoverTableFunc{})
}
