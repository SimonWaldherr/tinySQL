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

// A few real municipalities inside the Landkreis (coordinates from each
// town's German Wikipedia infobox), for the "Jump to a town" panel -- a
// starting point when exploring the tileset rather than an exhaustive list.
const TOWNS = [
    { name: 'Dingolfing', lon: 12.5, lat: 48.6333 },
    { name: 'Landau a.d. Isar', lon: 12.6939, lat: 48.6689 },
    { name: 'Reisbach', lon: 12.6333, lat: 48.5667 },
    { name: 'Wallersdorf', lon: 12.75, lat: 48.7333 },
    { name: 'Eichendorf', lon: 12.85, lat: 48.633 },
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
            throw new Error('no tile at ' + z + '/' + x + '/' + y);
        }
        const compressed = base64ToBytes(res.rows[0].tile_data);
        const bytes = await gunzip(compressed);
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
