// Bavaria demo: real OpenStreetMap vector tiles for Landkreis Dingolfing-
// Landau, fetched once as a tinySQL snapshot and then served entirely by SQL
// queries run through tinySQL compiled to WebAssembly -- MapLibre GL's
// vector-tile loading is redirected through a custom `tinysql://` protocol
// instead of a normal HTTP request. See tiles-demo.html/tiles-demo.js for
// the same idea against a small synthetic raster tileset, and
// .github/workflows/bavaria-mbtiles.yml for how this tileset is built.
'use strict';

// Published by .github/workflows/bavaria-mbtiles.yml directly into the
// gh-pages branch, rather than fetched from a GitHub Release at runtime.
// Release downloads redirect to a storage host without CORS headers, while
// this relative URL is served from the page's own origin.
const SNAPSHOT_URL = 'bavaria-snapshot.b64';

// Generous approximation of the Landkreis Dingolfing-Landau bounding box --
// see the same constant's comment in scripts/build-bavaria-tiles.sh for why
// this is a rectangle rather than the exact administrative polygon.
const DINGOLFING_BOUNDS = [12.25, 48.53, 12.90, 48.80];

// A few real municipalities inside the Landkreis (coordinates and population
// from each town's German Wikipedia infobox, population as of 31 Dec 2025),
// for the "Jump to a town" panel and the choropleth panel below -- a
// starting point when exploring the tileset rather than an exhaustive list.
const TOWNS = [
    { name: 'Dingolfing', lon: 12.5, lat: 48.6333, population: 20890 },
    { name: 'Landau a.d. Isar', lon: 12.6939, lat: 48.6689, population: 14776 },
    { name: 'Reisbach', lon: 12.6333, lat: 48.5667, population: 7673 },
    { name: 'Wallersdorf', lon: 12.75, lat: 48.7333, population: 7271 },
    { name: 'Eichendorf', lon: 12.85, lat: 48.633, population: 6671 },
];

// Vector layers a click can land on and pull real OSM tags from -- every
// filtered layer buildStyle() draws except 'background' and 'roads-casing'
// (a purely decorative outline under 'roads', not its own feature set),
// which carry no useful feature properties of their own.
const INSPECTABLE_LAYERS = ['buildings', 'roads', 'roads-minor', 'waterway', 'water', 'landuse', 'places'];

// The "Layers" panel's single "Roads" checkbox toggles all three road
// layers together -- casing, the major-class network and minor paths --
// since splitting the road styling into layers was a rendering choice, not
// something the toggle UI should have to know about.
const LAYER_GROUPS = { roads: ['roads-casing', 'roads', 'roads-minor'] };

const seenSQL = new Set();
const stats = { rendered: 0, repeats: 0, totalMs: 0, queries: 0 };
let map;
let mapExpanded = false;
let exploreLayerReady = false;
let tileFlashLayerReady = false;
let tileFlashCounter = 0;
// tileFlashFeatures backs the 'tile-flash' layer below -- pruned by both a
// hard cap and each flash's own removal timeout, so a fast pan never grows
// this without bound.
const tileFlashFeatures = [];

// Used only if tiles_metadata is missing minzoom/maxzoom -- every real
// tippecanoe/ExportMBTiles output writes both (see mbtilesWriteMetadata in
// internal/importer/mbtiles_export.go), so this only guards a malformed or
// hand-edited tileset rather than anything the normal pipeline produces.
const FALLBACK_MIN_ZOOM = 0;
const FALLBACK_MAX_ZOOM = 16;

function setStatus(text, state) {
    document.getElementById('statusText').textContent = text;
    const dot = document.getElementById('statusDot');
    dot.classList.toggle('ready', state === 'ready');
    dot.classList.toggle('error', state === 'error');
}

// runSQL mirrors tiles-demo.js's helper of the same name: execute through
// the WASM bridge, log it, track cache-eligibility stats. Every call site
// here builds SQL from integers tinySQL itself parsed out of the requested
// tile URL, so string interpolation carries the same risk profile as
// cmd/tinysqld's tile route and tiles-demo.js's tile layer.
function runSQL(sql, opts) {
    opts = opts || {};
    const t0 = performance.now();
    const res = window.executeQuery(sql);
    const ms = performance.now() - t0;

    const isRepeat = seenSQL.has(sql);
    seenSQL.add(sql);
    stats.queries++;
    stats.totalMs += ms;
    if (isRepeat) stats.repeats++;
    if (opts.kind === 'tile') stats.rendered++;

    logQuery(sql, ms, isRepeat);
    updateStats();
    return res;
}

function logQuery(sql, ms, isRepeat) {
    const log = document.getElementById('queryLog');
    const row = document.createElement('div');
    row.className = 'log-entry ' + (isRepeat ? 'repeat' : 'first');
    const tag = isRepeat ? 'CACHE-ELIGIBLE' : 'FIRST PARSE';
    row.innerHTML =
        '<span class="sql mono"></span>' +
        '<span class="meta"><span class="tag">' + tag + '</span><span>' + ms.toFixed(2) + ' ms</span></span>';
    row.querySelector('.sql').textContent = sql;
    log.insertBefore(row, log.firstChild);
    while (log.children.length > 30) log.removeChild(log.lastChild);
}

function updateStats() {
    document.getElementById('statRendered').textContent = String(stats.rendered);
    const pct = stats.queries ? Math.round((stats.repeats / stats.queries) * 100) : 0;
    document.getElementById('statRepeat').textContent = pct + '%';
    const avg = stats.queries ? stats.totalMs / stats.queries : 0;
    document.getElementById('statAvg').textContent = avg.toFixed(2) + ' ms';
    document.getElementById('queryCount').textContent = '(' + stats.queries + ' run)';
    updateTileLoadBadge();
}

// updateTileLoadBadge keeps the small overlay directly on the map (not just
// the sidebar, which is easy to not be looking at) in sync with how many
// tiles have actually been queried so far -- the same stats.rendered count
// the sidebar's "Tiles rendered" row shows, surfaced right where the
// flashes themselves appear so the two reinforce each other.
function updateTileLoadBadge() {
    const badge = document.getElementById('tileLoadBadge');
    if (!badge) return;
    badge.textContent = stats.rendered === 1
        ? '1 tile loaded via SQL — pan or zoom for more'
        : stats.rendered + ' tiles loaded via SQL — pan or zoom for more';
}

function loadMetadata() {
    const res = window.executeQuery('SELECT name, value FROM tiles_metadata ORDER BY name');
    const body = document.querySelector('#metaTable tbody');
    body.innerHTML = '';
    if (!res || !res.success || !res.rows) return;
    for (const row of res.rows) {
        const tr = document.createElement('tr');
        const k = document.createElement('td');
        k.textContent = row.name;
        const v = document.createElement('td');
        v.textContent = row.value;
        tr.appendChild(k);
        tr.appendChild(v);
        body.appendChild(tr);
    }
}

// loadZoomHistogram runs a real GROUP BY aggregate over the tiles table and
// renders one bar per zoom level, then compares the grand total against
// TILE_COUNT(maxZoom) -- how many tiles a fully covered world would need at
// just the deepest zoom level alone -- to make the tileset's clipping
// concrete: a single Landkreis needs a vanishingly small fraction of that.
function loadZoomHistogram() {
    const res = runSQL('SELECT zoom_level, COUNT(*) AS n FROM tiles GROUP BY zoom_level ORDER BY zoom_level', { kind: 'meta' });
    const container = document.getElementById('zoomHistogram');
    if (!container) return;
    container.innerHTML = '';
    if (!res || !res.success || !res.rows || !res.rows.length) return;

    const counts = res.rows.map((row) => ({ zoom: Number(row.zoom_level), n: Number(row.n) }));
    const maxN = Math.max(...counts.map((row) => row.n));
    const maxZoom = Math.max(...counts.map((row) => row.zoom));
    for (const { zoom, n } of counts) {
        const rowEl = document.createElement('div');
        rowEl.className = 'zoom-bar-row';
        const width = maxN ? Math.max(2, Math.round((n / maxN) * 100)) : 0;
        rowEl.innerHTML =
            '<span class="zoom-label">z' + zoom + '</span>' +
            '<span class="zoom-bar-track"><span class="zoom-bar-fill" style="width:' + width + '%"></span></span>' +
            '<span class="zoom-count">' + n.toLocaleString() + '</span>';
        container.appendChild(rowEl);
    }

    const totalRes = runSQL('SELECT COUNT(*) AS total FROM tiles', { kind: 'meta' });
    const possibleRes = runSQL('SELECT TILE_COUNT(' + maxZoom + ') AS possible', { kind: 'meta' });
    const note = document.getElementById('zoomCoverageNote');
    if (!note) return;
    if (totalRes && totalRes.success && totalRes.rows && totalRes.rows.length &&
        possibleRes && possibleRes.success && possibleRes.rows && possibleRes.rows.length) {
        const total = Number(totalRes.rows[0].total);
        const possible = Number(possibleRes.rows[0].possible);
        note.textContent =
            total.toLocaleString() + ' tiles stored across every zoom level. TILE_COUNT(' + maxZoom + ') says a ' +
            'fully covered world would need ' + possible.toLocaleString() + ' tiles at z' + maxZoom + ' alone — ' +
            'this whole clipped, single-Landkreis pyramid stores a vanishingly small fraction of that.';
    }
}

// readZoomRange pulls the tileset's own minzoom/maxzoom out of tiles_metadata
// rather than assuming a fixed constant. tippecanoe's -zg flag (see
// scripts/build-bavaria-tiles.sh) computes maxzoom from actual data density,
// so a hardcoded value here would drift from what was really built the next
// time the monthly CI job regenerates the tileset from fresher OSM data.
function readZoomRange() {
    const res = window.executeQuery("SELECT name, value FROM tiles_metadata WHERE name IN ('minzoom', 'maxzoom')");
    let minZoom = FALLBACK_MIN_ZOOM, maxZoom = FALLBACK_MAX_ZOOM;
    if (res && res.success && res.rows) {
        for (const row of res.rows) {
            const n = Number(row.value);
            if (!Number.isFinite(n)) continue;
            if (row.name === 'minzoom') minZoom = n;
            if (row.name === 'maxzoom') maxZoom = n;
        }
    }
    if (maxZoom < minZoom) maxZoom = minZoom;
    return { minZoom, maxZoom };
}

// base64ToBytes decodes the atob()-friendly text the WASM bridge returns for
// a TEXT column back into raw bytes -- see the comment on buildBrowserDB in
// cmd/mbtilesregion/main.go for why tile_data is base64 TEXT, not BLOB, in
// the first place.
function base64ToBytes(b64) {
    const binary = atob(b64);
    const bytes = new Uint8Array(binary.length);
    for (let i = 0; i < binary.length; i++) bytes[i] = binary.charCodeAt(i);
    return bytes;
}

// gunzip decompresses tile bytes client-side. This step is only necessary
// because the tinysql:// protocol bypasses fetch/XHR entirely: a normal
// HTTP tile request gets gzip decoded transparently by the browser from the
// Content-Encoding header (see cmd/tinysqld/tiles.go's own handling of this
// for the exact same reason), but a custom protocol handler hands MapLibre
// raw bytes directly, so nothing decodes them unless this does. tippecanoe
// gzips its vector tile output by default, matching the MBTiles
// specification's convention for pbf tiles.
async function gunzip(bytes) {
    if (typeof DecompressionStream === 'undefined') {
        // No fallback: every browser MapLibre GL itself supports also has
        // DecompressionStream, so this only matters for exotic embedders.
        throw new Error('this browser has no DecompressionStream; cannot decode gzip vector tiles');
    }
    const stream = new Blob([bytes]).stream().pipeThrough(new DecompressionStream('gzip'));
    return new Uint8Array(await new Response(stream).arrayBuffer());
}

// registerTinySQLProtocol makes MapLibre GL fetch vector tiles by running
// SQL instead of an HTTP request. tiles: ['tinysql://{z}/{x}/{y}'] in the
// style's source is what routes requests here.
// tileBoundsLonLat mirrors tile_functions.go's tileWestLon/tileNorthLat (the
// standard Web Mercator inverse) in plain JS, purely to position the "tile
// just loaded" flash rectangle below without an extra SQL round-trip on
// every single tile request -- TILE_BBOX already does the real thing
// elsewhere (see "Explore a point"), this is just a decorative overlay.
function tileBoundsLonLat(z, x, y) {
    const n = Math.pow(2, z);
    const west = (x / n) * 360 - 180;
    const east = ((x + 1) / n) * 360 - 180;
    const toLat = (row) => {
        const rad = Math.PI * (1 - (2 * row) / n);
        return (180 / Math.PI) * Math.atan(Math.sinh(rad));
    };
    return [west, toLat(y + 1), east, toLat(y)]; // [west, south, east, north], XYZ y
}

// ── "Loaded on demand" visualization ───────────────────────────────────────
//
// The whole point of this demo is that there is no preloaded tileset sitting
// in memory waiting to be revealed -- each tile MapLibre draws is the result
// of one SQL query, run at the moment it's first needed. Because that query
// runs against an already-imported in-memory table, it resolves in
// well under a millisecond, which paradoxically makes the "loaded on demand"
// behavior *invisible*: panning looks instantaneous, indistinguishable from
// everything having been fetched upfront. flashTileLoad makes each
// individual tile fetch visible: a colored rectangle appears over the tile
// that was *just* queried and fades out over tileFlashFadeMs -- an animation
// duration chosen for human perception, independent of how fast the query
// itself actually ran. Panning to a fresh area lights up tile by tile, the
// way a real remote tile server would look with visible network latency;
// panning back to an already-decoded area (MapLibre's own client-side tile
// cache, not tinySQL's) shows no flash at all, since no new query ran.
const tileFlashFadeMs = 700;
const tileFlashHoldMs = 60; // time at full opacity before the fade-out starts

function ensureTileFlashLayer() {
    if (tileFlashLayerReady) return;
    map.addSource('tile-flash', { type: 'geojson', data: { type: 'FeatureCollection', features: [] } });
    map.addLayer({
        id: 'tile-flash-fill', type: 'fill', source: 'tile-flash',
        paint: {
            'fill-color': ['case', ['get', 'found'], '#4ec9b0', '#f14c4c'],
            'fill-opacity': ['coalesce', ['feature-state', 'opacity'], 0],
            'fill-opacity-transition': { duration: tileFlashFadeMs, delay: 0 },
        },
    });
    map.addLayer({
        id: 'tile-flash-line', type: 'line', source: 'tile-flash',
        paint: {
            'line-color': ['case', ['get', 'found'], '#4ec9b0', '#f14c4c'],
            'line-width': 2,
            'line-opacity': ['coalesce', ['feature-state', 'opacity'], 0],
            'line-opacity-transition': { duration: tileFlashFadeMs, delay: 0 },
        },
    });
    tileFlashLayerReady = true;
}

// flashTileLoad briefly highlights the tile at bbox -- teal for a tile that
// was actually found and drawn, a thinner red dashed-feeling outline (via
// the same fill/line paint, lower peak opacity) for a coordinate that was
// queried but came back empty, so even "nothing here" is visibly a live
// query rather than a gap that was simply never asked about.
function flashTileLoad(bbox, found) {
    if (!map) return;
    ensureTileFlashLayer();
    const flashId = ++tileFlashCounter;
    const feature = {
        type: 'Feature',
        id: flashId,
        properties: { found },
        geometry: {
            type: 'Polygon',
            coordinates: [[
                [bbox[0], bbox[1]], [bbox[2], bbox[1]], [bbox[2], bbox[3]], [bbox[0], bbox[3]], [bbox[0], bbox[1]],
            ]],
        },
    };
    tileFlashFeatures.push(feature);
    if (tileFlashFeatures.length > 80) tileFlashFeatures.splice(0, tileFlashFeatures.length - 80);
    map.getSource('tile-flash').setData({ type: 'FeatureCollection', features: tileFlashFeatures });

    const peakOpacity = found ? 0.55 : 0.3;
    map.setFeatureState({ source: 'tile-flash', id: flashId }, { opacity: peakOpacity });
    // Two nested rAFs guarantee the peak-opacity state is actually painted
    // for at least one frame before the transition below starts animating
    // it back down -- otherwise the fade could start from 0 and the flash
    // would never visibly appear at all.
    requestAnimationFrame(() => {
        requestAnimationFrame(() => {
            map.setFeatureState({ source: 'tile-flash', id: flashId }, { opacity: 0 });
        });
    });
    setTimeout(() => {
        const idx = tileFlashFeatures.findIndex((f) => f.id === flashId);
        if (idx !== -1) {
            tileFlashFeatures.splice(idx, 1);
            map.getSource('tile-flash').setData({ type: 'FeatureCollection', features: tileFlashFeatures });
        }
    }, tileFlashHoldMs + tileFlashFadeMs + 200);
}

function registerTinySQLProtocol() {
    const urlPattern = /^tinysql:\/\/(\d+)\/(\d+)\/(\d+)$/;
    maplibregl.addProtocol('tinysql', async (params) => {
        const match = urlPattern.exec(params.url);
        if (!match) {
            throw new Error('malformed tinysql:// tile URL: ' + params.url);
        }
        const z = Number(match[1]);
        const x = Number(match[2]);
        const y = Number(match[3]);
        const sql =
            'SELECT tile_data FROM tiles WHERE zoom_level = ' + z +
            ' AND tile_column = ' + x +
            ' AND tile_row = TILE_FLIP_Y(' + y + ', ' + z + ') LIMIT 1';
        const res = runSQL(sql, { kind: 'tile' });
        if (!res || !res.success) {
            throw new Error((res && res.error) || 'query failed');
        }
        if (!res.rows || !res.rows.length || !res.rows[0].tile_data) {
            // A sparse tileset legitimately has no tile at some coordinates;
            // MapLibre treats a thrown error as "nothing to draw here", the
            // vector-tile equivalent of the raster demo's transparent PNG.
            flashTileLoad(tileBoundsLonLat(z, x, y), false);
            throw new Error('no tile at ' + z + '/' + x + '/' + y);
        }
        const compressed = base64ToBytes(res.rows[0].tile_data);
        const bytes = await gunzip(compressed);
        flashTileLoad(tileBoundsLonLat(z, x, y), true);
        return { data: bytes.buffer };
    });
}

// A compact style for the single "osm" source-layer scripts/build-bavaria-
// tiles.sh's tippecanoe invocation produces (-l osm): every OSM element in
// one layer, tagged with its original OSM keys as feature properties, so
// styling is filter-based (["has","building"], etc.) rather than per-
// source-layer. Property names follow osmium export's defaults and may need
// adjusting once a real tileset is available to inspect -- see this file's
// header comment.
function buildStyle(zoomRange) {
    // Past the tileset's real maxzoom, MapLibre automatically overzooms the
    // last available tile (re-scaling its real vector geometry) rather than
    // requesting one that doesn't exist, so the source's maxzoom is the
    // tileset's actual highest zoom, not a UI zoom limit -- see the "Zoom
    // levels" panel for the map's own, unrestricted zoom range.
    const buildingsMinZoom = Math.min(13, zoomRange.maxZoom);
    // Footways/tracks/cycleways outnumber real roads in rural OSM data and
    // drown the road network out at regional zoom, so they only draw once
    // zoomed in close -- clamped the same way as buildings, in case a
    // smaller tileset never reaches z14 at all.
    const minorRoadsMinZoom = Math.min(14, zoomRange.maxZoom);
    const MINOR_HIGHWAY = ['footway', 'path', 'track', 'cycleway', 'pedestrian', 'steps', 'bridleway', 'corridor'];
    const CASED_HIGHWAY = ['motorway', 'motorway_link', 'trunk', 'trunk_link', 'primary', 'primary_link'];

    return {
        version: 8,
        // MapLibre rejects a style outright -- every layer, not just the
        // labels -- if any layer's layout uses "text-field" without a
        // top-level "glyphs" template to fetch the label font from. The
        // 'places' layer below needs this to render at all.
        glyphs: 'https://demotiles.maplibre.org/font/{fontstack}/{range}.pbf',
        sources: {
            osm: {
                type: 'vector',
                tiles: ['tinysql://{z}/{x}/{y}'],
                minzoom: zoomRange.minZoom,
                maxzoom: zoomRange.maxZoom,
                bounds: DINGOLFING_BOUNDS,
            },
        },
        layers: [
            { id: 'background', type: 'background', paint: { 'background-color': '#1a2117' } },
            {
                // landuse=* together with the natural=* land-cover tags OSM uses
                // instead of landuse for forests/scrub/heath/grassland (osmium
                // export keeps both keys as top-level feature properties), colored
                // per type rather than one flat green blanket over everything.
                id: 'landuse', type: 'fill', source: 'osm', 'source-layer': 'osm',
                filter: ['any', ['has', 'landuse'], ['in', ['get', 'natural'], ['literal', ['wood', 'scrub', 'heath', 'grassland', 'grass', 'wetland']]]],
                paint: {
                    'fill-color': [
                        'match', ['coalesce', ['get', 'natural'], ['get', 'landuse']],
                        'wood', '#24392a', 'forest', '#24392a',
                        'scrub', '#374a30', 'heath', '#4a4a34',
                        'grassland', '#3c4a2e', 'grass', '#3c4a2e', 'meadow', '#3c4a2e',
                        'farmland', '#464a34', 'farmyard', '#464a34',
                        'orchard', '#3f4d38', 'vineyard', '#42462f',
                        'wetland', '#33454a',
                        'residential', '#39352f', 'industrial', '#393c40', 'commercial', '#393c40', 'retail', '#393c40',
                        'cemetery', '#394331', 'allotments', '#3d4f38', 'quarry', '#49463f',
                        '#2e3527',
                    ],
                    'fill-opacity': 0.85,
                },
            },
            {
                id: 'water', type: 'fill', source: 'osm', 'source-layer': 'osm',
                filter: ['==', ['get', 'natural'], 'water'],
                paint: { 'fill-color': '#274b6d' },
            },
            {
                id: 'waterway', type: 'line', source: 'osm', 'source-layer': 'osm',
                filter: ['has', 'waterway'],
                paint: { 'line-color': '#3f7ab0', 'line-width': 1.2 },
            },
            {
                // A dark outline under only the most major road classes, drawn
                // beneath 'roads' -- the classic cartographic "cased road" look.
                id: 'roads-casing', type: 'line', source: 'osm', 'source-layer': 'osm',
                filter: ['in', ['get', 'highway'], ['literal', CASED_HIGHWAY]],
                paint: {
                    'line-color': '#0d0f10',
                    'line-width': ['interpolate', ['linear'], ['zoom'], 6, 1.6, 12, 5.5, 16, 11.5],
                },
            },
            {
                id: 'roads-minor', type: 'line', source: 'osm', 'source-layer': 'osm',
                filter: ['in', ['get', 'highway'], ['literal', MINOR_HIGHWAY]],
                paint: {
                    'line-color': '#5f7a68',
                    'line-width': ['interpolate', ['linear'], ['zoom'], 14, 0.5, 18, 2],
                    'line-dasharray': [2, 1.5],
                    'line-opacity': 0.75,
                },
                minzoom: minorRoadsMinZoom,
            },
            {
                id: 'roads', type: 'line', source: 'osm', 'source-layer': 'osm',
                filter: ['all', ['has', 'highway'], ['!', ['in', ['get', 'highway'], ['literal', MINOR_HIGHWAY]]]],
                paint: {
                    'line-color': [
                        'match', ['get', 'highway'],
                        ['motorway', 'motorway_link'], '#e8a33d',
                        ['trunk', 'trunk_link'], '#e0954a',
                        ['primary', 'primary_link'], '#d9a24a',
                        ['secondary', 'secondary_link'], '#cbb27a',
                        ['tertiary', 'tertiary_link'], '#bfae86',
                        ['residential', 'unclassified', 'living_street'], '#8f8a7d',
                        ['service'], '#6f695f',
                        '#8a8378',
                    ],
                    'line-width': [
                        'interpolate', ['linear'], ['zoom'],
                        6, ['match', ['get', 'highway'], ['motorway', 'trunk'], 1.2, ['primary'], 0.8, 0.4],
                        12, ['match', ['get', 'highway'], ['motorway', 'trunk'], 3.5, ['primary'], 2.5, ['secondary', 'tertiary'], 1.5, 0.7],
                        16, ['match', ['get', 'highway'], ['motorway', 'trunk'], 8, ['primary'], 6, ['secondary', 'tertiary'], 3.5, 1.8],
                    ],
                },
            },
            {
                id: 'buildings', type: 'fill', source: 'osm', 'source-layer': 'osm',
                filter: ['has', 'building'],
                paint: { 'fill-color': '#5b5147', 'fill-opacity': 0.85, 'fill-outline-color': '#372f27' },
                minzoom: buildingsMinZoom,
            },
            {
                id: 'places', type: 'symbol', source: 'osm', 'source-layer': 'osm',
                filter: ['all', ['has', 'place'], ['has', 'name']],
                layout: {
                    'text-field': ['get', 'name'],
                    'text-font': ['Noto Sans Regular'],
                    'text-size': [
                        'match', ['get', 'place'],
                        'city', 16, 'town', 14, 'village', 12,
                        ['hamlet', 'suburb', 'neighbourhood'], 10,
                        9,
                    ],
                },
                paint: { 'text-color': '#e8e8e8', 'text-halo-color': '#0d0f10', 'text-halo-width': 1.2 },
            },
        ],
    };
}

function initMap(zoomRange) {
    map = new maplibregl.Map({
        container: 'map',
        style: buildStyle(zoomRange),
        // bounds/fitBoundsOptions (rather than a fixed center+zoom:10) fits
        // the actual district rectangle to whatever the viewport turns out
        // to be, and caps at the tileset's own maxzoom so a small viewport
        // never fits past the zoom level real data exists at.
        bounds: DINGOLFING_BOUNDS,
        fitBoundsOptions: { padding: 24, maxZoom: zoomRange.maxZoom },
        attributionControl: false,
    });
    map.addControl(new maplibregl.NavigationControl(), 'top-left');
    map.addControl(new maplibregl.AttributionControl({
        customAttribution: '© <a href="https://www.openstreetmap.org/copyright" target="_blank" rel="noopener">OpenStreetMap contributors</a> · queried live with SQL by <a href="index.html">tinySQL</a>',
    }));

    document.getElementById('zoomRange').textContent = zoomRange.minZoom + ' – ' + zoomRange.maxZoom;
    const updateZoomReadout = () => {
        const z = map.getZoom();
        document.getElementById('zoomCurrent').textContent = 'z' + z.toFixed(1);
        document.querySelectorAll('#zoomButtons button[data-zoom]').forEach((btn) => {
            btn.classList.toggle('active', Math.round(z) === Number(btn.dataset.zoom));
        });
    };
    map.on('zoom', updateZoomReadout);
    map.on('load', updateZoomReadout);
    buildZoomButtons(zoomRange);
    map.on('click', onMapClick);

    // A pointer cursor over anything queryRenderedFeatures in onMapClick can
    // actually find is the map's own hint that clicking there does
    // something, without needing to read the sidebar copy first.
    map.on('load', () => {
        for (const id of INSPECTABLE_LAYERS) {
            map.on('mouseenter', id, () => { map.getCanvas().style.cursor = 'pointer'; });
            map.on('mouseleave', id, () => { map.getCanvas().style.cursor = ''; });
        }
    });
    // The choropleth panel adds its own MapLibre source/layers, which
    // requires the style to already be loaded -- same requirement
    // ensureExploreLayer's callers already guard for on click.
    map.on('load', initChoropleth);
}

// setLayerVisibility toggles one of buildStyle()'s layers on or off -- a
// plain MapLibre layout property, not a SQL query, since which layers draw
// is a client-side rendering choice over vector tiles already fetched.
// layerId can name a LAYER_GROUPS entry (e.g. 'roads', which is really three
// layers -- casing, major, minor) so the "Layers" panel's single checkbox
// per concept doesn't need to know how many real layers back it.
function setLayerVisibility(layerId, visible) {
    if (!map) return;
    for (const id of LAYER_GROUPS[layerId] || [layerId]) {
        if (map.getLayer(id)) map.setLayoutProperty(id, 'visibility', visible ? 'visible' : 'none');
    }
}

// flyToTown centers the map on one of TOWNS -- a starting point for
// exploring the tileset, not a SQL-backed lookup (there is no place/address
// search over the vector tiles themselves, only whatever the current
// viewport has already fetched and decoded).
function flyToTown(lon, lat) {
    if (!map) return;
    map.flyTo({ center: [lon, lat], zoom: Math.max(map.getZoom(), 13) });
}

// ── Choropleth: population-density KPI, computed live in SQL ──────────────
//
// TOWNS' Wikipedia-sourced population figures are real; the region each town
// is colored by is a uniform CHOROPLETH_BUFFER_METERS-radius GEO_BUFFER
// circle around its point, NOT a real municipal boundary -- this tileset
// carries per-OSM-element data (buildings, roads, ...), not administrative
// boundary polygons, so there is nothing to dissolve a real boundary from.
// Density is therefore an illustrative approximation, not a statistic to
// cite -- the point of this panel is that GEO_BUFFER, GEO_POLYGON_AREA, and
// EQUAL_INTERVAL/NATURAL_BREAKS/NTILE are real, live SQL running against
// tinySQL-WASM for every class change, not client-side JavaScript math.
const CHOROPLETH_BUFFER_METERS = 3000;
const CHOROPLETH_CLASSES = 3;
// One hue (blue), light -> dark: the standard sequential/magnitude encoding
// for a choropleth legend. Class 1 is the lowest-density bucket.
const CHOROPLETH_COLORS = ['#9ec5f4', '#3987e5', '#104281'];
const CHOROPLETH_CLASS_LABELS = ['Lower density', 'Medium density', 'Higher density'];
const CHOROPLETH_METHODS = {
    natural_breaks: 'NATURAL_BREAKS',
    equal_interval: 'EQUAL_INTERVAL',
    quantile: 'NTILE',
};
let choroplethLayerReady = false;

// setupChoroplethTable creates and populates the `towns` table once, through
// ordinary INSERT statements (GEO_POINT builds each row's GEOMETRY value),
// exactly the way any real dataset would land in tinySQL.
function setupChoroplethTable() {
    runSQL('CREATE TABLE towns (name TEXT, geom GEOMETRY, population INT)', { kind: 'choropleth' });
    for (const t of TOWNS) {
        const name = t.name.replace(/'/g, "''");
        runSQL(
            "INSERT INTO towns VALUES ('" + name + "', GEO_POINT(" + t.lon + ', ' + t.lat + '), ' + t.population + ')',
            { kind: 'choropleth' }
        );
    }
}

// runChoroplethQuery computes each town's buffer region and density in an
// inner query, then classifies the density column with whichever window
// function the sidebar selector chose. The classifier reads the inner
// query's `density` column directly (a derived table, not a repeated
// expression), so GEO_BUFFER/GEO_POLYGON_AREA each run exactly once per row.
function runChoroplethQuery(methodKey) {
    const fn = CHOROPLETH_METHODS[methodKey] || CHOROPLETH_METHODS.natural_breaks;
    const sql =
        'SELECT name, population, region, density, ' +
        fn + '(' + CHOROPLETH_CLASSES + ') OVER (ORDER BY density) AS bucket ' +
        'FROM (SELECT name, population, GEO_BUFFER(geom, ' + CHOROPLETH_BUFFER_METERS + ') AS region, ' +
        'population / (GEO_POLYGON_AREA(GEO_BUFFER(geom, ' + CHOROPLETH_BUFFER_METERS + ')) / 1000000.0) AS density ' +
        'FROM towns) t ORDER BY name';
    return runSQL(sql, { kind: 'choropleth' });
}

// ensureChoroplethLayer lazily adds the fill+outline layers for the towns'
// buffer regions, colored by the `bucket` property SQL just computed.
function ensureChoroplethLayer() {
    if (choroplethLayerReady) return;
    map.addSource('choropleth', { type: 'geojson', data: { type: 'FeatureCollection', features: [] } });
    map.addLayer({
        id: 'choropleth-fill', type: 'fill', source: 'choropleth',
        paint: {
            'fill-color': [
                'match', ['get', 'bucket'],
                1, CHOROPLETH_COLORS[0], 2, CHOROPLETH_COLORS[1], 3, CHOROPLETH_COLORS[2],
                '#888888',
            ],
            'fill-opacity': 0.55,
        },
    });
    map.addLayer({
        id: 'choropleth-outline', type: 'line', source: 'choropleth',
        paint: { 'line-color': '#1e1e1e', 'line-width': 1 },
    });
    choroplethLayerReady = true;
}

// renderChoroplethLegend rebuilds the legend swatches and the per-town
// density table from the classified rows SQL just returned.
function renderChoroplethLegend(rows) {
    const legend = document.getElementById('choroplethLegend');
    const statsBody = document.querySelector('#choroplethStats tbody');
    if (!legend || !statsBody) return;

    legend.innerHTML = '';
    for (let i = 0; i < CHOROPLETH_CLASSES; i++) {
        const row = document.createElement('div');
        row.className = 'toggle-row';
        row.innerHTML = '<span class="legend-dot" style="background:' + CHOROPLETH_COLORS[i] + ';"></span>' + CHOROPLETH_CLASS_LABELS[i];
        legend.appendChild(row);
    }

    statsBody.innerHTML = '';
    const sorted = rows.slice().sort((a, b) => Number(b.density) - Number(a.density));
    for (const row of sorted) {
        const tr = document.createElement('tr');
        const name = document.createElement('td');
        name.textContent = row.name;
        const density = document.createElement('td');
        density.textContent = Number(row.density).toFixed(0) + ' /km²';
        tr.appendChild(name);
        tr.appendChild(density);
        statsBody.appendChild(tr);
    }
}

// setChoroplethMethod re-runs the classification query for the chosen
// method and redraws the map layer and legend -- called on page load with
// the default method, and again every time the sidebar selector changes.
function setChoroplethMethod(methodKey) {
    if (!map) return;
    const res = runChoroplethQuery(methodKey);
    if (!res || !res.success || !res.rows) return;
    ensureChoroplethLayer();

    const features = [];
    for (const row of res.rows) {
        let region;
        try {
            region = JSON.parse(row.region);
        } catch (err) {
            continue;
        }
        features.push({
            type: 'Feature',
            properties: {
                name: row.name,
                population: Number(row.population),
                density: Number(row.density),
                bucket: Number(row.bucket),
            },
            geometry: region,
        });
    }
    map.getSource('choropleth').setData({ type: 'FeatureCollection', features });
    renderChoroplethLegend(res.rows);
}

function initChoropleth() {
    setupChoroplethTable();
    setChoroplethMethod(document.getElementById('choroplethMethod')?.value || 'natural_breaks');
}

// ensureExploreLayer lazily adds a GeoJSON source/layer for outlining
// whatever tile onMapClick's TILE_BBOX result covers. Lazy because it needs
// the style to be loaded, and the first click could in principle arrive
// before that -- see the isStyleLoaded guard in onMapClick.
function ensureExploreLayer() {
    if (exploreLayerReady) return;
    map.addSource('explore', { type: 'geojson', data: { type: 'FeatureCollection', features: [] } });
    map.addLayer({
        id: 'explore-outline', type: 'line', source: 'explore',
        paint: { 'line-color': '#4ec9b0', 'line-width': 2, 'line-dasharray': [2, 2] },
    });
    exploreLayerReady = true;
}

function outlineTileBBox(bbox) {
    ensureExploreLayer();
    map.getSource('explore').setData({
        type: 'FeatureCollection',
        features: [{
            type: 'Feature',
            properties: {},
            geometry: {
                type: 'Polygon',
                coordinates: [[
                    [bbox[0], bbox[1]], [bbox[2], bbox[1]], [bbox[2], bbox[3]], [bbox[0], bbox[3]], [bbox[0], bbox[1]],
                ]],
            },
        }],
    });
}

// describeFeature lists the real OSM tags (osmium export's property names)
// of whatever vector feature sits under the clicked pixel -- the payoff of
// this being an actual OpenStreetMap extract rather than the generative-art
// demo's synthetic tiles. Returns what it found so onMapClick can also show
// it as a map popup, rather than querying the same pixel twice.
function describeFeature(point) {
    const box = document.getElementById('featureResult');
    const features = map.queryRenderedFeatures(point, { layers: INSPECTABLE_LAYERS });
    if (!features.length) {
        if (box) box.textContent = 'No OSM feature under this pixel — vector features are sparse points/lines/polygons; try a road, a building, or a place label.';
        return null;
    }
    const feature = features[0];
    const props = feature.properties || {};
    const keys = Object.keys(props).filter((k) => props[k] !== null && props[k] !== '').slice(0, 10);
    const lines = [feature.layer.id + ' feature, ' + keys.length + ' tag(s):'];
    for (const k of keys) lines.push('  ' + k + ' = ' + props[k]);
    if (box) box.textContent = lines.join('\n');
    return { layerId: feature.layer.id, props, keys };
}

function escapeHTML(value) {
    return String(value).replace(/[&<>"']/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
}

let clickPopup = null;

// showClickPopup is the map-visible half of a click, right at the point
// clicked, so clicking the map visibly does something on the map itself --
// the sidebar's "Explore a point" panel has the same information in more
// detail, but it's easy to miss if you're looking at the map, not the
// sidebar. Feature tag values are real, user-editable OSM data, so they're
// HTML-escaped before going into a popup built with setHTML.
function showClickPopup(lngLat, described, tilePresent) {
    if (clickPopup) clickPopup.remove();
    let html;
    if (described) {
        const name = described.props.name ? '<strong>' + escapeHTML(described.props.name) + '</strong><br>' : '';
        const rest = described.keys.filter((k) => k !== 'name').slice(0, 5)
            .map((k) => escapeHTML(k) + ' = ' + escapeHTML(described.props[k]))
            .join('<br>');
        html = name + '<span class="popup-layer">' + escapeHTML(described.layerId) + '</span>' + (rest ? '<br>' + rest : '');
    } else {
        html = 'No OSM feature here' +
            (tilePresent === false ? '<br><span class="popup-layer">no tile stored at this z/x/y either</span>' : '');
    }
    clickPopup = new maplibregl.Popup({ closeButton: true, closeOnClick: true, maxWidth: '240px', className: 'click-popup' })
        .setLngLat(lngLat)
        .setHTML(html)
        .addTo(map);
}

// onMapClick runs TILE_ZXY/TILE_QUADKEY/TILE_BBOX/TILE_PARENT for the
// clicked point at the current integer zoom, checks whether that exact
// tile is actually stored (a clipped, real-world tileset is sparse outside
// its region), outlines the tile, and separately inspects whatever OSM
// feature sits under the pixel.
function onMapClick(e) {
    if (!map.isStyleLoaded()) return;
    const lat = e.lngLat.lat;
    const lng = e.lngLat.lng;
    const z = Math.max(0, Math.min(30, Math.round(map.getZoom())));

    const zxyRes = runSQL('SELECT TILE_ZXY(' + lng + ', ' + lat + ', ' + z + ') AS zxy', { kind: 'point' });
    const out = document.getElementById('pointResult');
    if (!out) return;
    if (!zxyRes || !zxyRes.success || !zxyRes.rows || !zxyRes.rows.length) {
        out.textContent = (zxyRes && zxyRes.error) || 'query failed';
        return;
    }
    let zxy;
    try {
        zxy = JSON.parse(zxyRes.rows[0].zxy);
    } catch (err) {
        out.textContent = 'could not parse TILE_ZXY result';
        return;
    }

    const detailRes = runSQL(
        'SELECT TILE_QUADKEY(' + zxy.z + ', ' + zxy.x + ', ' + zxy.y + ') AS quadkey, ' +
            'TILE_BBOX(' + zxy.z + ', ' + zxy.x + ', ' + zxy.y + ') AS bbox, ' +
            'TILE_PARENT(' + zxy.z + ', ' + zxy.x + ', ' + zxy.y + ') AS parent',
        { kind: 'point' }
    );
    const presentRes = runSQL(
        'SELECT COUNT(*) AS n FROM tiles WHERE zoom_level = ' + zxy.z +
            ' AND tile_column = ' + zxy.x + ' AND tile_row = ' + zxy.tile_row,
        { kind: 'point' }
    );

    let quadkey = '?', bbox = null, parent = null;
    if (detailRes && detailRes.success && detailRes.rows && detailRes.rows.length) {
        quadkey = detailRes.rows[0].quadkey;
        try { bbox = JSON.parse(detailRes.rows[0].bbox); } catch (err) { bbox = null; }
        try { parent = detailRes.rows[0].parent ? JSON.parse(detailRes.rows[0].parent) : null; } catch (err) { parent = null; }
    }
    const present = (presentRes && presentRes.success && presentRes.rows && presentRes.rows.length)
        ? Number(presentRes.rows[0].n) > 0 : null;

    out.textContent =
        'z/x/y      ' + zxy.z + ' / ' + zxy.x + ' / ' + zxy.y + '\n' +
        'tile_row   ' + zxy.tile_row + '  (TMS, for a direct tiles-table lookup)\n' +
        'quadkey    ' + quadkey + '\n' +
        (parent ? 'parent     ' + parent.z + ' / ' + parent.x + ' / ' + parent.y + '\n' : '') +
        (present !== null ? 'in tiles?  ' + (present ? 'yes — stored exactly at this z/x/y' : 'no — sparse tileset, nothing here') + '\n' : '') +
        (bbox ? 'bbox       [' + bbox.map((n) => n.toFixed(4)).join(', ') + ']' : '');

    if (bbox) outlineTileBBox(bbox);
    const described = describeFeature(e.point);
    showClickPopup(e.lngLat, described, present);
}

// buildZoomButtons adds one preset per quartile of the tileset's *actual*
// zoom range (not a guessed constant) so "different zoom levels" always
// means the real levels this particular tileset was built at, however the
// monthly CI job's -zg guess happens to land.
function buildZoomButtons(zoomRange) {
    const container = document.getElementById('zoomButtons');
    document.querySelectorAll('#zoomButtons button[data-zoom]').forEach((btn) => btn.remove());
    const span = zoomRange.maxZoom - zoomRange.minZoom;
    const steps = span >= 3 ? 4 : Math.max(2, span + 1);
    const seen = new Set();
    for (let i = 0; i < steps; i++) {
        const z = Math.round(zoomRange.minZoom + (span * i) / (steps - 1));
        if (seen.has(z)) continue;
        seen.add(z);
        const btn = document.createElement('button');
        btn.className = 'btn secondary';
        btn.textContent = 'z' + z;
        btn.dataset.zoom = String(z);
        btn.onclick = () => map.easeTo({ zoom: z });
        container.appendChild(btn);
    }
}

function fitRegion() {
    if (!map) return;
    map.fitBounds(DINGOLFING_BOUNDS, { padding: 24 });
}

// toggleMapExpanded mirrors tiles-demo.js's toggle of the same name: hide
// the sidebar and collapse the topbar to free up both axes for the map,
// then tell MapLibre to recompute its canvas size (CSS alone doesn't).
function toggleMapExpanded() {
    mapExpanded = !mapExpanded;
    document.getElementById('sidebar').classList.toggle('collapsed', mapExpanded);
    document.getElementById('topbar').classList.toggle('collapsed', mapExpanded);
    document.getElementById('expandBtn').textContent = mapExpanded ? '⤡ Restore layout' : '⤢ Expand map';
    if (map) setTimeout(() => map.resize(), 0);
}

async function instantiateWasm(go) {
    const wasmURL = 'query_files.wasm';
    if (typeof DecompressionStream !== 'undefined') {
        try {
            const compressed = await fetch(wasmURL + '.gz');
            if (!compressed.ok || !compressed.body) {
                throw new Error('compressed WASM unavailable (' + compressed.status + ')');
            }
            const stream = compressed.body.pipeThrough(new DecompressionStream('gzip'));
            return await WebAssembly.instantiateStreaming(
                new Response(stream, { headers: { 'Content-Type': 'application/wasm' } }),
                go.importObject
            );
        } catch (error) {
            console.info('Compressed WASM unavailable, using standard loader:', error);
        }
    }
    if (WebAssembly.instantiateStreaming) {
        try {
            return await WebAssembly.instantiateStreaming(fetch(wasmURL), go.importObject);
        } catch (error) {
            console.warn('instantiateStreaming failed, falling back to ArrayBuffer:', error);
        }
    }
    const response = await fetch(wasmURL);
    if (!response.ok) throw new Error('WASM request failed (' + response.status + ')');
    const bytes = await response.arrayBuffer();
    return WebAssembly.instantiate(bytes, go.importObject);
}

async function main() {
    registerTinySQLProtocol();

    const go = new Go();
    try {
        const result = await instantiateWasm(go);
        go.run(result.instance || result);
    } catch (error) {
        setStatus('Failed to load WASM: ' + error.message, 'error');
        return;
    }

    setStatus('Fetching tileset snapshot…', null);
    let snapshotText;
    try {
        const response = await fetch(SNAPSHOT_URL);
        if (!response.ok) {
            throw new Error('HTTP ' + response.status);
        }
        snapshotText = (await response.text()).trim();
    } catch (error) {
        setStatus(
            'Failed to fetch the same-origin tileset snapshot (' + error.message + '). ' +
            'It is published by a monthly GitHub Actions job and may not exist yet on a fresh fork — see .github/workflows/bavaria-mbtiles.yml.',
            'error'
        );
        return;
    }

    if (typeof window.importDatabase !== 'function' || !snapshotText) {
        setStatus('Demo data failed to load', 'error');
        return;
    }
    const imported = window.importDatabase(snapshotText);
    if (!imported || !imported.success) {
        setStatus('Failed to load tileset: ' + (imported && imported.error), 'error');
        return;
    }

    loadMetadata();
    initMap(readZoomRange());
    loadZoomHistogram();
    setStatus('Ready — pan and zoom the map', 'ready');
}

main().catch((error) => {
    console.error('tiles-demo-bavaria fatal error:', error);
    setStatus('Fatal error: ' + error.message, 'error');
});
