#!/usr/bin/env bash
set -euo pipefail

# build-bavaria-tiles.sh: download a real regional OSM extract, clip it down
# to Dingolfing-Landau, and build a vector .mbtiles tileset from it.
#
# This is the shell half of the "real data" MBTiles demo requested alongside
# cmd/mbtilesdemo's synthetic one: fetch, extract, tile. tinySQL itself has
# no OSM-PBF parser and no cartographic renderer, so this script leans on the
# two standard, purpose-built external tools for that:
#
#   - osmium-tool (https://osmcode.org/osmium-tool/): clips the Niederbayern
#     region down to Dingolfing-Landau, then exports the result to GeoJSON
#     Text Sequences (RFC 8142) with OSM tags preserved as feature properties.
#   - tippecanoe (https://github.com/felt/tippecanoe): builds the actual
#     vector tiles from that GeoJSON sequence.
#
# Go's role starts only once a real .mbtiles file exists: cmd/mbtilesregion
# imports it through tinySQL's own ImportMBTiles and writes the base64
# snapshot tiles-demo-bavaria.js loads at runtime.
#
# Usage:
#   ./scripts/build-bavaria-tiles.sh [output-dir]
#
# Requires osmium-tool and tippecanoe on PATH (see .github/workflows/
# bavaria-mbtiles.yml for how CI installs both) and Go with the sqliteimport
# build tag available.
#
# NOTE ON THE BOUNDING BOX: DINGOLFING_BBOX below is a generous rectangular
# approximation of the Landkreis Dingolfing-Landau, not its exact
# administrative boundary -- osmium extract also accepts --polygon=<file>
# for a precise clip; fetching that polygon (e.g. from the OSM relation via
# Overpass/Nominatim) is a worthwhile follow-up but adds a network
# dependency this script deliberately avoids for a first version. A
# generous bbox errs toward including a little of the neighboring
# Landkreise rather than cutting off part of Dingolfing-Landau itself.

OUT_DIR="${1:-build/bavaria}"
GEOFABRIK_URL="https://download.geofabrik.de/europe/germany/bayern/niederbayern-latest.osm.pbf"
DINGOLFING_BBOX="12.25,48.53,12.90,48.80" # minlon,minlat,maxlon,maxlat

mkdir -p "$OUT_DIR"
cd "$OUT_DIR"

for tool in osmium tippecanoe curl; do
  if ! command -v "$tool" >/dev/null 2>&1; then
    echo "error: $tool not found on PATH" >&2
    exit 1
  fi
done

echo "==> downloading $GEOFABRIK_URL"
if [ ! -f niederbayern-latest.osm.pbf ]; then
  curl -fL --retry 3 -o niederbayern-latest.osm.pbf "$GEOFABRIK_URL"
else
  echo "    already present, skipping download"
fi

echo "==> extracting Dingolfing-Landau (bbox $DINGOLFING_BBOX)"
osmium extract --bbox="$DINGOLFING_BBOX" --overwrite \
  -o dingolfing-landau.osm.pbf niederbayern-latest.osm.pbf

echo "==> exporting to GeoJSON Text Sequence (OSM tags as properties)"
osmium export --output-format=geojsonseq --overwrite \
  -o dingolfing-landau.geojsonseq dingolfing-landau.osm.pbf

echo "==> building vector tiles with tippecanoe"
rm -f dingolfing-landau.mbtiles
tippecanoe \
  -o dingolfing-landau.mbtiles \
  -l osm \
  -zg \
  --read-parallel \
  --drop-densest-as-needed \
  --name="Dingolfing-Landau" \
  --attribution="© OpenStreetMap contributors" \
  --description="Dingolfing-Landau (Bavaria, Germany), extracted from Geofabrik's niederbayern-latest.osm.pbf" \
  dingolfing-landau.geojsonseq

echo "==> done: $OUT_DIR/dingolfing-landau.mbtiles"
ls -lh dingolfing-landau.mbtiles
