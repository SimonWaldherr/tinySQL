//go:build sqliteimport && !js && !wasm && !baremetal

package main

// mapshaperShape is a deliberately small, representative editing layer for
// the browser demo. It includes a line, a polygon with a hole, and a
// multipolygon so all of the Mapshaper-style geometry operations are visible.
type mapshaperShape struct {
	Name        string
	Kind        string
	Geometry    string
	Description string
}

func buildMapshaperShapes() []mapshaperShape {
	return []mapshaperShape{
		{
			Name: "coastline", Kind: "LineString",
			Geometry:    `{"type":"LineString","coordinates":[[-150,-30],[-141,-23],[-132,-27],[-123,-17],[-114,-21],[-105,-10],[-96,-15],[-87,-7],[-78,-12]]}`,
			Description: "A deliberately angular coast for simplification and smoothing.",
		},
		{
			Name: "lagoon", Kind: "Polygon with a hole",
			Geometry:    `{"type":"Polygon","coordinates":[[[35,-13],[48,-19],[66,-13],[74,1],[65,17],[46,20],[31,7],[35,-13]],[[47,-4],[57,-6],[63,2],[57,9],[46,7],[43,1],[47,-4]]]}`,
			Description: "An island outline with an interior lagoon to demonstrate hole removal.",
		},
		{
			Name: "archipelago", Kind: "MultiPolygon",
			Geometry:    `{"type":"MultiPolygon","coordinates":[[[[105,28],[117,24],[125,33],[121,43],[109,44],[101,36],[105,28]],[[110,32],[116,32],[118,37],[113,39],[108,36],[110,32]]],[[[133,19],[143,17],[150,25],[146,34],[136,34],[130,27],[133,19]]]]}`,
			Description: "Two islands, one with an inland lake, for multi-part geometry editing.",
		},
	}
}
