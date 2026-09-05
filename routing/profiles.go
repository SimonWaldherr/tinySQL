package routing

import (
	"fmt"
	"math"
	"strconv"
	"strings"
)

func profileKeys(p Profile) []string {
	switch p {
	case Car:
		return []string{"access", "vehicle", "motor_vehicle", "motorcar"}
	case Bicycle:
		return []string{"access", "vehicle", "bicycle"}
	default:
		return []string{"access", "foot"}
	}
}
func access(tags map[string]string, p Profile, direction string, base bool) (bool, error) {
	value := ""
	for _, key := range profileKeys(p) {
		if v := tags[key]; v != "" {
			value = v
		}
		if direction != "" {
			if v := tags[key+":"+direction]; v != "" {
				value = v
			}
		}
	}
	switch value {
	case "":
		return base, nil
	case "yes", "permissive", "designated", "official":
		return true, nil
	case "dismount":
		return p == Bicycle || p == Foot, nil
	case "no", "private", "destination", "customers", "delivery", "agricultural", "forestry", "use_sidepath":
		return false, nil
	default:
		return false, fmt.Errorf("unsupported access value %q", value)
	}
}

var roadSpeeds = map[string]float64{"motorway": 100, "motorway_link": 50, "trunk": 80, "trunk_link": 40, "primary": 60, "primary_link": 40, "secondary": 50, "secondary_link": 35, "tertiary": 40, "tertiary_link": 30, "unclassified": 40, "residential": 30, "living_street": 7, "service": 15, "track": 10, "road": 20}

func defaultSpeed(highway string, p Profile) (float64, bool) {
	if highway == "construction" || highway == "proposed" || highway == "raceway" || highway == "platform" || highway == "" {
		return 0, false
	}

	if p == Car {
		v, ok := roadSpeeds[highway]
		return v, ok
	}
	if highway == "motorway" || highway == "motorway_link" || highway == "trunk" || highway == "trunk_link" {
		return 0, false
	}
	_, road := roadSpeeds[highway]
	path := highway == "footway" || highway == "pedestrian" || highway == "path" || highway == "cycleway" || highway == "steps" || highway == "bridleway"
	if !road && !path {
		return 0, false
	}
	if p == Foot {
		return 5, true
	}
	if highway == "steps" || highway == "footway" || highway == "pedestrian" {
		return 5, false
	}
	return 18, true
}
func parseSpeed(value string, base float64) (float64, error) {
	if value == "" {
		return base, nil
	}
	if value == "walk" {
		return math.Min(base, 5), nil
	}
	multiplier := 1.0
	value = strings.TrimSpace(value)
	if strings.HasSuffix(value, "mph") {
		multiplier = 1.609344
		value = strings.TrimSpace(strings.TrimSuffix(value, "mph"))
	} else {
		value = strings.TrimSpace(strings.TrimSuffix(value, "km/h"))
	}
	v, err := strconv.ParseFloat(value, 64)
	if err != nil || math.IsNaN(v) || math.IsInf(v, 0) || v <= 0 {
		return 0, fmt.Errorf("unsupported maxspeed %q", value)
	}
	return math.Min(base, v*multiplier), nil
}
func waySpeeds(w Way, p Profile) (float64, float64, error) {
	speed, base := defaultSpeed(w.Tags["highway"], p)
	// Explicit mode permission may open a path, but never construction/proposed.
	if speed == 0 {
		switch w.Tags["highway"] {
		case "footway", "cycleway", "path", "pedestrian", "steps", "bridleway":
			speed = 5
		default:
			return 0, 0, nil
		}
	}
	for key := range w.Tags {
		if strings.Contains(key, ":conditional") {
			return 0, 0, fmt.Errorf("way %d: conditional tag %q needs a time-aware profile", w.ID, key)
		}
	}
	if w.Tags["area"] == "yes" {
		return 0, 0, nil
	}
	forward, err := access(w.Tags, p, "forward", base)
	if err != nil {
		return 0, 0, err
	}
	backward, err := access(w.Tags, p, "backward", base)
	if err != nil {
		return 0, 0, err
	}
	one := ""
	if p != Foot {
		one = w.Tags["oneway"]
		if one == "" && (w.Tags["junction"] == "roundabout" || w.Tags["highway"] == "motorway") {
			one = "yes"
		}
	}
	if v := w.Tags["oneway:"+string(p)]; v != "" {
		one = v
	}
	if p == Car {
		for _, key := range []string{"oneway:motor_vehicle", "oneway:motorcar"} {
			if v := w.Tags[key]; v != "" {
				one = v
			}
		}
	}
	if p == Bicycle && w.Tags["oneway:bicycle"] == "" {
		for _, key := range []string{"cycleway", "cycleway:left", "cycleway:right"} {
			if strings.HasPrefix(w.Tags[key], "opposite") {
				one = "no"
			}
		}
	}
	switch one {
	case "", "no", "0", "false":
	case "yes", "1", "true":
		backward = false
	case "-1", "reverse":
		forward = false
	default:
		return 0, 0, fmt.Errorf("way %d: unsupported oneway %q", w.ID, one)
	}
	if p == Bicycle && w.Tags["bicycle"] == "dismount" {
		speed = 5
	}
	if p == Car {
		for _, key := range []string{"maxheight", "maxwidth", "maxweight", "maxlength", "maxaxleload"} {
			if w.Tags[key] != "" {
				return 0, 0, fmt.Errorf("way %d: %s requires a vehicle-specific profile", w.ID, key)
			}
		}
	}
	f, b := speed, speed
	if p == Car {
		var err error
		f, err = parseSpeed(w.Tags["maxspeed"], f)
		if err != nil {
			return 0, 0, fmt.Errorf("way %d: %w", w.ID, err)
		}
		b = f
		f, err = parseSpeed(w.Tags["maxspeed:forward"], f)
		if err != nil {
			return 0, 0, err
		}
		b, err = parseSpeed(w.Tags["maxspeed:backward"], b)
		if err != nil {
			return 0, 0, err
		}
	}
	if !forward {
		f = 0
	}
	if !backward {
		b = 0
	}
	return f / 3.6, b / 3.6, nil
}
func nodeAllowed(n Node, p Profile) (bool, error) {
	base := true
	if n.Tags["barrier"] != "" {
		switch n.Tags["barrier"] {
		case "entrance", "cattle_grid":
		case "bollard", "block", "cycle_barrier":
			base = p == Foot
		case "stile", "turnstile":
			base = p == Foot
		default:
			base = false
		}
	}
	for key := range n.Tags {
		if strings.Contains(key, ":conditional") {
			return false, fmt.Errorf("node %d: conditional access unsupported", n.ID)
		}
	}
	return access(n.Tags, p, "", base)
}
