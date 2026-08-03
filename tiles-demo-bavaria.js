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

const seenSQL = new Set();
const stats = { rendered: 0, repeats: 0, totalMs: 0, queries: 0 };
let map;

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
function buildStyle() {
    return {
        version: 8,
        sources: {
            osm: {
                type: 'vector',
                tiles: ['tinysql://{z}/{x}/{y}'],
                minzoom: 0,
                maxzoom: 14,
                bounds: DINGOLFING_BOUNDS,
            },
        },
        layers: [
            { id: 'background', type: 'background', paint: { 'background-color': '#16181a' } },
            {
                id: 'landuse', type: 'fill', source: 'osm', 'source-layer': 'osm',
                filter: ['has', 'landuse'],
                paint: { 'fill-color': '#33472f', 'fill-opacity': 0.55 },
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
                id: 'buildings', type: 'fill', source: 'osm', 'source-layer': 'osm',
                filter: ['has', 'building'],
                paint: { 'fill-color': '#5b5147', 'fill-opacity': 0.85 },
                minzoom: 13,
            },
            {
                id: 'roads', type: 'line', source: 'osm', 'source-layer': 'osm',
                filter: ['has', 'highway'],
                paint: {
                    'line-color': '#d7c9a7',
                    'line-width': ['interpolate', ['linear'], ['zoom'], 8, 0.5, 16, 3],
                },
            },
            {
                id: 'places', type: 'symbol', source: 'osm', 'source-layer': 'osm',
                filter: ['has', 'place'],
                layout: { 'text-field': ['get', 'name'], 'text-size': 12, 'text-font': ['Noto Sans Regular'] },
                paint: { 'text-color': '#e8e8e8', 'text-halo-color': '#000', 'text-halo-width': 1 },
            },
        ],
    };
}

function initMap() {
    map = new maplibregl.Map({
        container: 'map',
        style: buildStyle(),
        center: [(DINGOLFING_BOUNDS[0] + DINGOLFING_BOUNDS[2]) / 2, (DINGOLFING_BOUNDS[1] + DINGOLFING_BOUNDS[3]) / 2],
        zoom: 10,
        attributionControl: false,
    });
    map.addControl(new maplibregl.NavigationControl(), 'top-left');
    map.addControl(new maplibregl.AttributionControl({
        customAttribution: '© <a href="https://www.openstreetmap.org/copyright" target="_blank" rel="noopener">OpenStreetMap contributors</a> · queried live with SQL by <a href="index.html">tinySQL</a>',
    }));
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
    initMap();
    setStatus('Ready — pan and zoom the map', 'ready');
}

main().catch((error) => {
    console.error('tiles-demo-bavaria fatal error:', error);
    setStatus('Fatal error: ' + error.message, 'error');
});
