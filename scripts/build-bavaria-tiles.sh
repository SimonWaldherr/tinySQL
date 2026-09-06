#!/usr/bin/env bash
set -euo pipefail

# build-bavaria-tiles.sh downloads a regional OSM extract, clips it to
# Dingolfing-Landau, and builds a vector MBTiles tileset.
#
# Usage:
#   ./scripts/build-bavaria-tiles.sh [output-dir]

OUT_DIR="${1:-build/bavaria}"
GEOFABRIK_URL="https://download.geofabrik.de/europe/germany/bayern/niederbayern-latest.osm.pbf"
# This is intentionally a generous rectangular approximation. It avoids an
# additional network dependency for an administrative-boundary polygon.
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
