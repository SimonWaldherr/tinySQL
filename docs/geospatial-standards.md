# Geospatial standards and profiles

tinySQL's geospatial APIs are designed around published encodings and
identifier rules, not around individual portals, vendors, or service URLs. A
dataset from a Bavarian authority, a German federal service, an INSPIRE node,
or a self-hosted pipeline follows the same code path when it uses the same
standard.

## Compatibility principles

1. **Preserve before converting.** Native CRS geometry stays native unless a
   caller requests and can perform an explicit reprojection. In particular,
   projected metre coordinates are never relabelled as GeoJSON longitude and
   latitude.
2. **Discover from standard metadata.** GeoPackage layers come from
   `gpkg_contents`, `gpkg_geometry_columns`, and `gpkg_spatial_ref_sys`; tile
   calculations use TileMatrix parameters rather than endpoint-specific
   presets.
3. **Keep identifiers portable.** CRS labels, OGC URNs, and OGC definition
   URIs normalize to one canonical identifier. Provider aliases do not become
   engine APIs.
4. **Make ambiguity explicit.** Multi-layer packages require a layer choice;
   unknown WMS 1.3 axis order can be supplied explicitly as `xy` or `yx`.
5. **Keep heavy capabilities optional.** SQLite-container access is behind the
   `sqliteimport` build profile; scalar GeoPackageBinary, CRS, WKB, WMS, and
   TileMatrix functions remain dependency-free.

## Implemented standards

| Area | Standard/profile | tinySQL support |
| --- | --- | --- |
| Feature JSON | RFC 7946 GeoJSON | Import, export, validated `GEOMETRY`, predicates and editing |
| Simple features | OGC WKT/WKB and EWKT/EWKB interoperability | 2D, Z, M and ZM decoding; M is intentionally dropped when converting to GeoJSON |
| Portable GIS container | OGC GeoPackage 1.x | Catalog inspection, selected feature-layer import, GeoPackageBinary inspection, WKB extraction, WGS84-to-GeoJSON conversion |
| Map services | OGC WMS 1.1/1.3 | Version-aware BBOX formatting and CRS axis order |
| Tiled maps | OGC WMTS / Two Dimensional Tile Matrix Set | Scale/resolution helpers plus arbitrary top-left or bottom-left TileMatrix addressing |
| CRS identifiers | OGC definition URI and URN conventions, EPSG labels | Normalization, canonical URI, axis metadata and JSON description |
| European profiles | INSPIRE ETRS89-LAEA/LCC and north-east UTM variants | Recognized CRS identifiers and normative axis order (for example EPSG:3034, 3035, 3044, 3045) |
| German profiles | AdV/BKG ETRS89/DREF91, UTM and DHDN/Gauss-Krüger variants | Recognized east/north and north/east variants, including current DREF91/2016 identifiers |
| Virtual globes | OGC KML | Placemark, ExtendedData and common geometry import |
| Tile archives | MBTiles | Import/export, metadata, TMS/XYZ conversion and read-only artifacts |

Recognition of a CRS means identifier and axis-order interoperability. It does
not imply that `ST_TRANSFORM` implements that projection. The built-in
reprojection remains deliberately limited to EPSG:4326 and EPSG:3857; other
coordinate operations should use an explicit PROJ/GDAL step so datum grids and
accuracy metadata are not approximated invisibly.

## GeoPackage geometry modes

`ImportOptions.GeoPackageGeometryMode` controls only the geometry column:

| Mode | Behavior |
| --- | --- |
| `auto` | Decode EPSG:4326 to GeoJSON; preserve every other CRS as GeoPackageBinary |
| `geojson` | Require EPSG:4326 and decode; reject other or unknown CRSs |
| `wkb` | Remove the GeoPackage header, retain native OGC WKB coordinates |
| `gpkg` / `native` | Preserve the complete GeoPackageBinary BLOB |

Use `InspectGeoPackage` before importing a multi-layer file. The selected
layer's standard catalog metadata is written to `<target>_metadata`, including
source layer, geometry type, SRS definition, bounds, timestamp and encoding
mode. SRS identifiers `0` and `-1` retain their GeoPackage meaning as
undefined geographic and Cartesian systems; neither is interpreted as WGS84.
For standard GeoPackageBinary rows, the importer rejects EWKB SRID flags and
checks that the decoded geometry is assignable to the catalog geometry type.
In tolerant mode such rows are reported and skipped; `StrictTypes: true`
aborts immediately.

## Priorities for further interoperability

The next additions should continue in standards order:

1. GML 3.2 Simple Features streaming as the common foundation for WFS 2.0,
   INSPIRE application schemas, XPlanGML and NAS/AAA exchange data.
2. ISO 19115/19139 and GeoDCAT-AP metadata extraction into a normalized source
   catalog with licence, lineage, temporal extent and quality fields.
3. OGC API - Features/Tiles/Maps landing-page and conformance parsing based on
   advertised links, without hard-coded hosts.
4. Cloud Optimized GeoTIFF window/overview metadata and coverage reads, while
   keeping raster rendering outside the SQL core.
5. Optional PROJ-backed coordinate operations with declared source/target CRS,
   grid availability and reported transformation accuracy.
