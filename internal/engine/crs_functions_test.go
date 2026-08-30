package engine

import (
	"encoding/json"
	"testing"
)

func TestCRSIdentifierNormalization(t *testing.T) {
	cases := map[string]string{
		"CRS_NORMALIZE(25832)":                                           "EPSG:25832",
		"CRS_NORMALIZE('epsg:25832')":                                    "EPSG:25832",
		"CRS_NORMALIZE('urn:ogc:def:crs:EPSG::25832')":                   "EPSG:25832",
		"CRS_NORMALIZE('https://www.opengis.net/def/crs/EPSG/0/25832')":  "EPSG:25832",
		"CRS_NORMALIZE('http://www.opengis.net/gml/srs/epsg.xml#25832')": "EPSG:25832",
		"CRS_NORMALIZE('OGC:CRS84')":                                     "CRS:84",
		"CRS_URI('urn:ogc:def:crs:EPSG:10.095:3035')":                    "http://www.opengis.net/def/crs/EPSG/0/3035",
		"CRS_URI('CRS:84')":                                              "http://www.opengis.net/def/crs/OGC/1.3/CRS84",
	}
	for sql, want := range cases {
		if got := tileEval(t, sql); got != want {
			t.Errorf("%s = %v, want %q", sql, got, want)
		}
	}
}

func TestCRSAxisOrderGermanAndEuropeanProfiles(t *testing.T) {
	cases := map[string]any{
		"CRS_AXIS_ORDER('CRS:84')":      "longitude,latitude",
		"CRS_AXIS_ORDER('EPSG:4326')":   "latitude,longitude",
		"CRS_AXIS_ORDER('EPSG:25832')":  "easting,northing",
		"CRS_AXIS_ORDER('EPSG:3035')":   "northing,easting",
		"CRS_AXIS_ORDER('EPSG:3044')":   "northing,easting",
		"CRS_AXIS_ORDER('EPSG:31468')":  "northing,easting",
		"CRS_AXIS_ORDER('EPSG:5678')":   "easting,northing",
		"CRS_AXIS_ORDER('EPSG:999999')": nil,
	}
	for sql, want := range cases {
		if got := tileEval(t, sql); got != want {
			t.Errorf("%s = %v, want %v", sql, got, want)
		}
	}

	raw := tileEval(t, "CRS_INFO('EPSG:10732')").(string)
	var info struct {
		Authority string   `json:"authority"`
		Code      string   `json:"code"`
		Canonical string   `json:"canonical"`
		Axes      []string `json:"axes"`
		Unit      string   `json:"unit"`
	}
	if err := json.Unmarshal([]byte(raw), &info); err != nil {
		t.Fatalf("CRS_INFO result: %v", err)
	}
	if info.Authority != "EPSG" || info.Code != "10732" || info.Canonical != "EPSG:10732" ||
		len(info.Axes) != 2 || info.Axes[0] != "easting" || info.Unit != "metre" {
		t.Fatalf("unexpected CRS_INFO: %#v", info)
	}
}

func TestCRSIdentifierValidation(t *testing.T) {
	for _, sql := range []string{
		"CRS_NORMALIZE('')",
		"CRS_NORMALIZE(0)",
		"CRS_NORMALIZE('0')",
		"CRS_NORMALIZE('EPSG:0')",
		"CRS_NORMALIZE('BAYERN:UTM32')",
		"CRS_NORMALIZE(25832.5)",
		"CRS_NORMALIZE(NULL)",
	} {
		if err := tileEvalErr(t, sql); err == nil {
			t.Errorf("%s should fail", sql)
		}
	}
}
