// MBTiles demo: every map tile is one SQL query, run by tinySQL compiled to
// WebAssembly. See tiles-demo.html for the query template and
// cmd/mbtilesdemo (repo root) for how the tileset itself was generated.
'use strict';

const TRANSPARENT_PNG =
    'iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNkYAAAAAYAAjCB0C8AAAAASUVORK5CYII=';
const MAX_ZOOM = 4;

const seenSQL = new Set();
const stats = { rendered: 0, repeats: 0, totalMs: 0, queries: 0 };
let wasmReady = false;
let map, boundsLayer;

function setStatus(text, ready) {
    document.getElementById('statusText').textContent = text;
    document.getElementById('statusDot').classList.toggle('ready', !!ready);
}

// runSQL executes SQL through the WASM bridge and logs it to the sidebar. Every
// call site in this file builds SQL from values tinySQL itself already
// validated as integers (tile z/x/y) or that the user typed into a native
// number input (lat/lng from a map click), so string interpolation here
// carries the same risk profile as cmd/tinysqld's tile route.
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

    logQuery(sql, ms, isRepeat, opts.kind);
    updateStats();
    return res;
}

function logQuery(sql, ms, isRepeat, kind) {
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

function wrapTileX(x, z) {
    const n = 1 << z;
    return ((x % n) + n) % n;
}

const TinySQLTileLayer = L.GridLayer.extend({
    createTile: function (coords) {
        const tile = document.createElement('img');
        tile.width = 256;
        tile.height = 256;
        tile.alt = '';

        const n = 1 << coords.z;
        if (coords.y < 0 || coords.y >= n) {
            tile.src = 'data:image/png;base64,' + TRANSPARENT_PNG;
            return tile;
        }
        const x = wrapTileX(coords.x, coords.z);
        const sql =
            'SELECT tile_data FROM tiles WHERE zoom_level = ' + coords.z +
            ' AND tile_column = ' + x +
            ' AND tile_row = TILE_FLIP_Y(' + coords.y + ', ' + coords.z + ') LIMIT 1';
        const res = runSQL(sql, { kind: 'tile' });
        if (res && res.success && res.rows && res.rows.length && res.rows[0].tile_data) {
            tile.src = 'data:image/png;base64,' + res.rows[0].tile_data;
        } else {
            tile.src = 'data:image/png;base64,' + TRANSPARENT_PNG;
        }
        return tile;
    },
});

function initMap() {
    map = L.map('map', {
        center: [15, 20],
        zoom: 2,
        minZoom: 0,
        maxZoom: MAX_ZOOM,
        maxBounds: [[-89.5, -Infinity], [89.5, Infinity]],
        maxBoundsViscosity: 0.8,
        worldCopyJump: false,
        attributionControl: false,
        zoomControl: true,
    });
    new TinySQLTileLayer({
        tileSize: 256,
        minZoom: 0,
        maxZoom: MAX_ZOOM,
        noWrap: false,
    }).addTo(map);
    L.control.attribution({ prefix: false })
        .addAttribution('Generated tileset &middot; queried live with SQL by <a href="index.html">tinySQL</a>')
        .addTo(map);

    map.on('click', onMapClick);
}

function onMapClick(e) {
    const z = map.getZoom();
    const lat = e.latlng.lat;
    const lng = ((e.latlng.lng + 180) % 360 + 360) % 360 - 180;

    const zxyRes = runSQL(
        'SELECT TILE_ZXY(' + lng + ', ' + lat + ', ' + z + ') AS zxy',
        { kind: 'point' }
    );
    const out = document.getElementById('pointResult');
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
            'TILE_BBOX(' + zxy.z + ', ' + zxy.x + ', ' + zxy.y + ') AS bbox',
        { kind: 'point' }
    );

    let quadkey = '?', bbox = null;
    if (detailRes && detailRes.success && detailRes.rows && detailRes.rows.length) {
        quadkey = detailRes.rows[0].quadkey;
        try {
            bbox = JSON.parse(detailRes.rows[0].bbox);
        } catch (err) {
            bbox = null;
        }
    }

    out.textContent =
        'z/x/y      ' + zxy.z + ' / ' + zxy.x + ' / ' + zxy.y + '\n' +
        'tile_row   ' + zxy.tile_row + '  (TMS, for a direct tiles-table lookup)\n' +
        'quadkey    ' + quadkey +
        (bbox ? '\nbbox       [' + bbox.map((n) => n.toFixed(3)).join(', ') + ']' : '');

    if (bbox) {
        if (boundsLayer) map.removeLayer(boundsLayer);
        boundsLayer = L.rectangle(
            [[bbox[1], bbox[0]], [bbox[3], bbox[2]]],
            { color: '#4ec9b0', weight: 2, fillOpacity: 0.08 }
        ).addTo(map);
    }
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
    // WASM and the tileset must be ready before the map exists: Leaflet
    // renders the initial viewport's tiles synchronously inside
    // layer.addTo(map), so creating the map first would call
    // window.executeQuery before go.run() has registered it.
    const go = new Go();
    try {
        const result = await instantiateWasm(go);
        go.run(result.instance || result);
    } catch (error) {
        setStatus('Failed to load WASM: ' + error.message, false);
        return;
    }
    wasmReady = true;

    if (typeof window.importDatabase !== 'function' || typeof window.TILES_DEMO_SNAPSHOT !== 'string') {
        setStatus('Demo data failed to load', false);
        return;
    }
    const imported = window.importDatabase(window.TILES_DEMO_SNAPSHOT);
    if (!imported || !imported.success) {
        setStatus('Failed to load tileset: ' + (imported && imported.error), false);
        return;
    }

    loadMetadata();
    initMap();
    setStatus('Ready — pan and zoom the map', true);
}

main().catch((error) => {
    console.error('tiles-demo fatal error:', error);
    setStatus('Fatal error: ' + error.message, false);
});
