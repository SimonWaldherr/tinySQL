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
let settlementMarkers = new Map(); // name -> L.CircleMarker
let selectedSettlements = []; // up to 2 settlement names, in click order
let routeLayer = null, midpointMarker = null;
let shapeSourceLayer = null, shapeResultLayer = null;
let shapeBoundsLayer = null, shapeCentroidMarker = null;
let uploadedShape = null, lastShapeGeometry = null, lastShapeName = 'shape';
let editorMode = false;
let customMarkers = new Map(); // name -> L.Marker

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
    map.on('contextmenu', onMapContextMenu);
}

// ── Editor mode: place/remove named markers with real INSERT/DELETE ───────
//
// This is deliberately small: a `custom_markers` table alongside `tiles` and
// `settlements`, one row per pin, edited with ordinary SQL. It exists as a
// foundation someone could build a simple map-based game or scenario editor
// on top of (place units, resources, waypoints, ...), not a game itself.

function ensureCustomMarkersTable() {
    runSQL('CREATE TABLE IF NOT EXISTS custom_markers (name TEXT, geometry JSON)', { kind: 'editor' });
}

function loadCustomMarkers() {
    const res = runSQL('SELECT name, geometry FROM custom_markers ORDER BY name', { kind: 'editor' });
    if (!res || !res.success || !res.rows) return;
    for (const row of res.rows) {
        let point;
        try {
            point = JSON.parse(row.geometry);
        } catch (err) {
            continue;
        }
        addCustomMarkerToMap(row.name, point.coordinates[1], point.coordinates[0]);
    }
}

function setEditorMode(on) {
    editorMode = on;
    document.getElementById('editorHint').textContent = on
        ? 'On — click the map to name and place a marker; right-click a marker to remove it.'
        : 'Off — clicking the map explores a point as usual.';
}

// addCustomMarker prompts for a label, INSERTs it, and renders the pin. A
// blank or cancelled prompt adds nothing.
function addCustomMarker(lat, lng) {
    const name = window.prompt('Name this marker:', '');
    if (!name || !name.trim()) return;
    const trimmed = name.trim();
    const sql =
        'INSERT INTO custom_markers VALUES (' + quoteSQL(trimmed) + ', GEO_POINT(' + lng + ', ' + lat + '))';
    const res = runSQL(sql, { kind: 'editor' });
    if (!res || !res.success) {
        setGeoResult('Could not place marker: ' + ((res && res.error) || 'unknown error'));
        return;
    }
    addCustomMarkerToMap(trimmed, lat, lng);
}

function addCustomMarkerToMap(name, lat, lng) {
    const marker = L.marker([lat, lng], {
        icon: L.divIcon({
            className: '',
            html: '<div style="width:12px;height:12px;background:#c586c0;border:2px solid #fff;' +
                'border-radius:2px;transform:rotate(45deg);box-shadow:0 0 0 1px #1e1e1e;"></div>',
            iconSize: [12, 12],
        }),
    }).addTo(map);
    marker.bindTooltip(name, { direction: 'top', offset: [0, -6] });
    marker.on('contextmenu', (ev) => {
        L.DomEvent.stopPropagation(ev);
        L.DomEvent.preventDefault(ev.originalEvent);
        showContextMenu(ev.originalEvent.pageX, ev.originalEvent.pageY, [
            { label: 'Delete "' + name + '"', onClick: () => removeCustomMarker(name) },
        ]);
    });
    customMarkers.set(name, marker);
}

function removeCustomMarker(name) {
    const sql = 'DELETE FROM custom_markers WHERE name = ' + quoteSQL(name);
    const res = runSQL(sql, { kind: 'editor' });
    if (!res || !res.success) {
        setGeoResult('Could not delete marker: ' + ((res && res.error) || 'unknown error'));
        return;
    }
    const marker = customMarkers.get(name);
    if (marker) map.removeLayer(marker);
    customMarkers.delete(name);
}

// ── Right-click context menu ───────────────────────────────────────────────

function showContextMenu(pageX, pageY, items) {
    const menu = document.getElementById('contextMenu');
    menu.innerHTML = '';
    for (const item of items) {
        const el = document.createElement('div');
        el.className = 'ctx-item';
        el.textContent = item.label;
        el.onclick = () => {
            hideContextMenu();
            item.onClick();
        };
        menu.appendChild(el);
    }
    // Keep the menu on-screen even when the click is near an edge.
    const maxX = window.innerWidth - 200;
    const maxY = window.innerHeight - (items.length * 32 + 12);
    menu.style.left = Math.max(4, Math.min(pageX, maxX)) + 'px';
    menu.style.top = Math.max(4, Math.min(pageY, maxY)) + 'px';
    menu.style.display = 'block';
}

function hideContextMenu() {
    document.getElementById('contextMenu').style.display = 'none';
}

document.addEventListener('click', hideContextMenu);
document.addEventListener('keydown', (e) => {
    if (e.key === 'Escape') hideContextMenu();
});

// onMapContextMenu offers the same lookups a left click runs, plus placing a
// marker regardless of whether editor mode is toggled on -- right-click is
// an explicit, deliberate action, so it always offers it.
function onMapContextMenu(e) {
    L.DomEvent.preventDefault(e.originalEvent);
    const lat = e.latlng.lat;
    const lng = ((e.latlng.lng + 180) % 360 + 360) % 360 - 180;
    showContextMenu(e.originalEvent.pageX, e.originalEvent.pageY, [
        { label: 'Explore this point (TILE_ZXY)', onClick: () => onMapClick({ latlng: e.latlng }, true) },
        { label: 'Nearest settlement', onClick: () => findNearestSettlement(lng, lat) },
        { label: 'Add marker here', onClick: () => addCustomMarker(lat, lng) },
        { label: 'Copy coordinates', onClick: () => copyCoordinates(lat, lng) },
    ]);
}

function copyCoordinates(lat, lng) {
    const text = lat.toFixed(6) + ', ' + lng.toFixed(6);
    if (navigator.clipboard && navigator.clipboard.writeText) {
        navigator.clipboard.writeText(text).catch(() => {});
    }
    setGeoResult('Copied coordinates: ' + text);
}

// biomeColor gives each settlement marker a color matching the terrain band
// it was placed on (see biomeFor in cmd/mbtilesdemo/settlements.go), so the
// points read as consistent with the map under them rather than arbitrary.
const BIOME_COLORS = {
    Grassland: '#8fbf5e',
    Plains: '#c9b458',
    Forest: '#3f7a4a',
    Highland: '#9a938c',
};

// loadSettlements queries the settlements table tinySQL holds alongside the
// tiles table (see addSettlementsTable in cmd/mbtilesdemo/main.go) and adds
// one CircleMarker per row. Clicking a marker feeds the "Geospatial queries"
// panel below instead of the tile/point-explore panel above.
function loadSettlements() {
    const res = runSQL(
        'SELECT name, geometry, biome, population FROM settlements ORDER BY name',
        { kind: 'settlements' }
    );
    if (!res || !res.success || !res.rows) return;
    for (const row of res.rows) {
        let point;
        try {
            point = JSON.parse(row.geometry);
        } catch (err) {
            continue;
        }
        const [lon, lat] = point.coordinates;
        const marker = L.circleMarker([lat, lon], {
            radius: 5,
            color: '#1e1e1e',
            weight: 1,
            fillColor: BIOME_COLORS[row.biome] || '#c7d0da',
            fillOpacity: 0.9,
        }).addTo(map);
        marker.bindTooltip(row.name + ' &middot; ' + row.biome + ' &middot; pop. ' + row.population, { direction: 'top', offset: [0, -4] });
        marker.on('click', (ev) => {
            L.DomEvent.stopPropagation(ev); // don't also trigger the map's own click handler
            onSettlementClick(row.name);
        });
        settlementMarkers.set(row.name, marker);
    }
    document.getElementById('settlementCount').textContent = String(res.rows.length);
}

// onSettlementClick builds up to a 2-settlement selection. The first click
// picks the "from" point; a second, *different* marker runs GEO_DISTANCE/
// GEO_BEARING/GEO_MIDPOINT between the two through one live SQL query and
// draws the great-circle midpoint on the map. Clicking a third marker (or
// re-clicking one of the pair) starts a new pair from scratch.
function onSettlementClick(name) {
    if (selectedSettlements.length === 1 && selectedSettlements[0] !== name) {
        selectedSettlements.push(name);
    } else {
        for (const n of selectedSettlements) highlightSettlement(n, false);
        selectedSettlements = [name];
    }
    highlightSettlement(name, true);

    if (selectedSettlements.length < 2) {
        setGeoResult('Selected ' + selectedSettlements[0] + ' — click a second settlement to measure between them.');
        return;
    }
    runSettlementGeoQuery(selectedSettlements[0], selectedSettlements[1]);
}

function highlightSettlement(name, on) {
    const marker = settlementMarkers.get(name);
    if (!marker) return;
    marker.setStyle(on ? { radius: 8, weight: 2, color: '#4ec9b0' } : { radius: 5, weight: 1, color: '#1e1e1e' });
}

// runSettlementGeoQuery is the one query that demonstrates GEO_DISTANCE,
// GEO_BEARING and GEO_MIDPOINT together, against real table rows rather than
// literal coordinates -- the self-join pattern documented in the README's
// "Geospatial queries" section.
function runSettlementGeoQuery(fromName, toName) {
    const sql =
        "SELECT GEO_DISTANCE(a.geometry, b.geometry) AS meters, " +
        "GEO_BEARING(a.geometry, b.geometry) AS bearing, " +
        "GEO_MIDPOINT(a.geometry, b.geometry) AS midpoint " +
        "FROM settlements a JOIN settlements b ON a.name = '" + fromName.replace(/'/g, "''") + "' " +
        "AND b.name = '" + toName.replace(/'/g, "''") + "'";
    const res = runSQL(sql, { kind: 'geo' });
    if (!res || !res.success || !res.rows || !res.rows.length) {
        setGeoResult((res && res.error) || 'query failed');
        return;
    }
    const row = res.rows[0];
    const km = (row.meters / 1000).toFixed(1);
    let midLon = null, midLat = null;
    try {
        const mid = JSON.parse(row.midpoint);
        [midLon, midLat] = mid.coordinates;
    } catch (err) {
        // leave midpoint unset; the numeric fields below still render.
    }
    setGeoResult(
        fromName + ' → ' + toName + '\n' +
        'distance     ' + km + ' km  (' + Math.round(row.meters) + ' m)\n' +
        'bearing      ' + row.bearing.toFixed(1) + '° from ' + fromName + '\n' +
        (midLon !== null ? 'midpoint     ' + midLat.toFixed(3) + ', ' + midLon.toFixed(3) : '')
    );

    if (routeLayer) map.removeLayer(routeLayer);
    if (midpointMarker) map.removeLayer(midpointMarker);
    const fromLatLng = settlementMarkers.get(fromName).getLatLng();
    const toLatLng = settlementMarkers.get(toName).getLatLng();
    routeLayer = L.polyline([fromLatLng, toLatLng], { color: '#4ec9b0', weight: 2, dashArray: '4 4' }).addTo(map);
    if (midLon !== null) {
        midpointMarker = L.circleMarker([midLat, midLon], {
            radius: 4, color: '#fff', weight: 1, fillColor: '#4ec9b0', fillOpacity: 1,
        }).addTo(map);
    }
}

function setGeoResult(text) {
    document.getElementById('geoResult').textContent = text;
}

function quoteSQL(value) {
    return "'" + String(value).replace(/'/g, "''") + "'";
}

function shapeOperationExpression(operation) {
    const numeric = function (id, fallback) {
        const value = Number(document.getElementById(id).value);
        return Number.isFinite(value) ? value : fallback;
    };
    switch (operation) {
    case 'simplify':
        return "ST_SIMPLIFY(geometry, " + numeric('simplifyTolerance', 2) + ", 'visvalingam-weighted')";
    case 'smooth':
        return 'ST_SMOOTH(geometry, ' + numeric('smoothIterations', 1) + ')';
    case 'affine':
        return 'ST_AFFINE(geometry, ' + numeric('affineShiftX', 8) + ', ' + numeric('affineShiftY', 0) +
            ', ' + numeric('affineScale', 0.8) + ', ' + numeric('affineRotate', 18) + ')';
    case 'drop-holes':
        return 'ST_REMOVE_HOLES(geometry)';
    case 'clean':
        return 'ST_CLEAN(geometry)';
    case 'snap':
        return 'ST_SNAPTOGRID(geometry, ' + numeric('snapGrid', 1) + ')';
    default:
        return 'geometry';
    }
}

function shapeOperationLabel(operation) {
    return {
        original: 'original geometry',
        simplify: 'ST_SIMPLIFY · weighted Visvalingam',
        smooth: 'ST_SMOOTH · one Chaikin pass',
        affine: 'ST_AFFINE · shift, scale and rotate',
        'drop-holes': 'ST_REMOVE_HOLES',
        clean: 'ST_CLEAN · remove repeated vertices and close rings',
        snap: 'ST_SNAPTOGRID · round to a coordinate grid',
    }[operation] || operation;
}

function clearShapeLayers() {
    for (const layer of [shapeSourceLayer, shapeResultLayer, shapeBoundsLayer, shapeCentroidMarker]) {
        if (layer) map.removeLayer(layer);
    }
    shapeSourceLayer = shapeResultLayer = shapeBoundsLayer = shapeCentroidMarker = null;
}

function addShapeLayer(geometry, options) {
    return L.geoJSON(geometry, {
        style: options,
        pointToLayer: function (_feature, latlng) {
            return L.circleMarker(latlng, { radius: 6, ...options });
        },
    }).addTo(map);
}

// runShapeOperation executes the editing expression and its inspection
// functions in one SQL statement. The source geometry remains in the result
// set so the map can compare the original and edited shapes side by side.
function runShapeOperation(operation) {
    if (!wasmReady || !map) return;
    const name = document.getElementById('shapeSelect').value;
    const expression = shapeOperationExpression(operation);
    const selectedSourceSQL = name === 'uploaded' && uploadedShape
        ? 'SELECT ' + quoteSQL(uploadedShape.name) + ' AS name, ' + quoteSQL(uploadedShape.kind) + ' AS kind, ' +
            quoteSQL(uploadedShape.description) + ' AS description, ' + quoteSQL(uploadedShape.geometry) + ' AS source_geometry, ' +
            expression.replace(/geometry/g, quoteSQL(uploadedShape.geometry)) + ' AS geometry'
        : 'SELECT name, kind, description, geometry AS source_geometry, ' + expression +
            ' AS geometry FROM mapshaper_shapes WHERE name = ' + quoteSQL(name);
    const sql =
        'WITH selected AS (' + selectedSourceSQL + ') ' +
        'SELECT name, kind, description, source_geometry, geometry, ST_ISVALID(geometry) AS is_valid, ST_BBOX(geometry) AS bbox, ' +
        'ST_CENTROID(geometry) AS centroid FROM selected';
    document.getElementById('shapeSQL').textContent = sql;
    const res = runSQL(sql, { kind: 'mapshaper' });
    const out = document.getElementById('shapeResult');
    if (!res || !res.success || !res.rows || !res.rows.length) {
        out.textContent = (res && res.error) || 'query failed';
        return;
    }

    const row = res.rows[0];
    let source, result, bbox, centroid;
    try {
        source = JSON.parse(row.source_geometry);
        result = JSON.parse(row.geometry);
        bbox = JSON.parse(row.bbox);
        centroid = JSON.parse(row.centroid);
    } catch (error) {
        out.textContent = 'could not parse geometry result: ' + error.message;
        return;
    }

    clearShapeLayers();
    shapeSourceLayer = addShapeLayer(source, {
        color: '#7a8291', weight: 2, dashArray: '5 4', fillOpacity: 0.03,
    });
    shapeResultLayer = addShapeLayer(result, {
        color: '#4ec9b0', weight: 3, fillColor: '#4ec9b0', fillOpacity: 0.18,
    });
    lastShapeGeometry = result;
    lastShapeName = row.name || 'shape';
    if (bbox && bbox.length === 4) {
        shapeBoundsLayer = L.rectangle(
            [[bbox[1], bbox[0]], [bbox[3], bbox[2]]],
            { color: '#c586c0', weight: 1, dashArray: '3 4', fillOpacity: 0 }
        ).addTo(map);
    }
    if (centroid && centroid.coordinates && centroid.coordinates.length >= 2) {
        shapeCentroidMarker = L.circleMarker([centroid.coordinates[1], centroid.coordinates[0]], {
            radius: 4, color: '#fff', weight: 1, fillColor: '#c586c0', fillOpacity: 1,
        }).addTo(map).bindTooltip('ST_CENTROID', { direction: 'top' });
    }

    document.querySelectorAll('[data-operation]').forEach((button) => {
        button.classList.toggle('active', button.dataset.operation === operation);
    });
    out.textContent =
        row.name + ' · ' + row.kind + '\n' + row.description + '\n' +
        'operation  ' + shapeOperationLabel(operation) + '\n' +
        'valid      ' + (row.is_valid ? 'yes' : 'no') + '\n' +
        'bbox       [' + bbox.map((n) => Number(n).toFixed(2)).join(', ') + ']\n' +
        'centroid   ' + Number(centroid.coordinates[1]).toFixed(2) + ', ' + Number(centroid.coordinates[0]).toFixed(2);
}

function loadShapeFile(file) {
    if (!file) return;
    const reader = new FileReader();
    reader.onerror = function () { document.getElementById('shapeResult').textContent = 'Could not read ' + file.name; };
    reader.onload = function () {
        try {
            const geometry = JSON.parse(String(reader.result));
            if (!geometry || typeof geometry.type !== 'string') throw new Error('expected a GeoJSON object with a type');
            uploadedShape = {
                name: file.name.replace(/\.[^.]+$/, '') || 'uploaded',
                kind: geometry.type,
                description: 'Uploaded from ' + file.name,
                geometry: JSON.stringify(geometry),
            };
            const select = document.getElementById('shapeSelect');
            const option = select.querySelector('option[value="uploaded"]');
            option.disabled = false;
            option.textContent = uploadedShape.name + ' · uploaded';
            select.value = 'uploaded';
            runShapeOperation('original');
        } catch (error) {
            document.getElementById('shapeResult').textContent = 'Invalid GeoJSON: ' + error.message;
        }
    };
    reader.readAsText(file);
}

function downloadShapeResult() {
    if (!lastShapeGeometry) {
        document.getElementById('shapeResult').textContent = 'Run an operation before downloading.';
        return;
    }
    const blob = new Blob([JSON.stringify(lastShapeGeometry, null, 2) + '\n'], { type: 'application/geo+json' });
    const link = document.createElement('a');
    link.href = URL.createObjectURL(blob);
    link.download = lastShapeName.replace(/[^a-z0-9_-]+/gi, '-') + '-edited.geojson';
    link.click();
    URL.revokeObjectURL(link.href);
}

// onMapClick explores the clicked point (TILE_ZXY/TILE_QUADKEY/TILE_BBOX +
// nearest settlement) unless editor mode is on, in which case a plain left
// click places a marker instead. forceExplore lets the context menu's
// "Explore this point" action run the lookup even while editor mode is on,
// without that click also being interpreted as "place a marker here".
function onMapClick(e, forceExplore) {
    const lat = e.latlng.lat;
    const lng = ((e.latlng.lng + 180) % 360 + 360) % 360 - 180;
    if (editorMode && !forceExplore) {
        addCustomMarker(lat, lng);
        return;
    }
    const z = map.getZoom();

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

    findNearestSettlement(lng, lat);
}

// findNearestSettlement runs GEO_DISTANCE against every row (there is no
// spatial index here -- see the README's Limitations on R-tree support --
// so this is ORDER BY + LIMIT 1, a full scan over a few dozen rows, not a
// seek) to show the clicked point's nearest settlement and its distance.
function findNearestSettlement(lng, lat) {
    const res = runSQL(
        'SELECT name, biome, GEO_DISTANCE(geometry, GEO_POINT(' + lng + ', ' + lat + ')) AS meters ' +
        'FROM settlements ORDER BY meters LIMIT 1',
        { kind: 'geo' }
    );
    if (!res || !res.success || !res.rows || !res.rows.length) return;
    const row = res.rows[0];
    const km = (row.meters / 1000).toFixed(1);
    setGeoResult('Nearest to click: ' + row.name + ' (' + row.biome + '), ' + km + ' km away.\nClick two settlement markers to measure between them.');
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
    loadSettlements();
    ensureCustomMarkersTable();
    loadCustomMarkers();
    runShapeOperation('original');
    setStatus('Ready — pan and zoom, measure settlements, or edit a shape', true);
}

main().catch((error) => {
    console.error('tiles-demo fatal error:', error);
    setStatus('Fatal error: ' + error.message, false);
});
