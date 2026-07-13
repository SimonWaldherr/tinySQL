// TinySQL Query Files - WASM Application
let wasmReady = false;
let currentTables = [];
let currentResults = null;
const HISTORY_KEY = 'tinysql_query_history_v1';
const DB_SNAPSHOT_KEY = 'tinysql_query_files_db_snapshot_v1';
const EDITOR_STATE_KEY = 'tinysql_query_files_editor_v1';
const RESULT_RENDER_LIMIT = 500;
const DEMO_HASH_PREFIX = 'demo=';
const SQL_KEYWORDS = [
    'SELECT', 'FROM', 'WHERE', 'JOIN', 'LEFT JOIN', 'RIGHT JOIN', 'FULL JOIN', 'INNER JOIN', 'CROSS JOIN',
    'ON', 'GROUP BY', 'ORDER BY', 'HAVING', 'LIMIT', 'OFFSET', 'DISTINCT', 'AS', 'AND', 'OR', 'NOT',
    'NULL', 'IN', 'EXISTS', 'CASE', 'WHEN', 'THEN', 'ELSE', 'END', 'LIKE', 'INSERT', 'UPDATE', 'DELETE',
    'CREATE TABLE', 'CREATE VIEW', 'CREATE MATERIALIZED VIEW', 'ALTER MATERIALIZED VIEW', 'REFRESH MATERIALIZED VIEW',
    'DROP VIEW', 'DROP MATERIALIZED VIEW', 'ALTER TABLE', 'DROP TABLE', 'UNION', 'UNION ALL', 'INTERSECT', 'EXCEPT', 'WITH',
    'COUNT', 'SUM', 'AVG', 'MIN', 'MAX', 'ROW_NUMBER', 'OVER', 'PARTITION BY', 'ASC', 'DESC', 'LIMIT',
    'PIVOT', 'RETURNING', 'EXPLAIN', 'PRAGMA',
    'ST_MAKEPOINT', 'ST_POINT', 'ST_X', 'ST_Y', 'ST_DISTANCE', 'ST_DWITHIN', 'ST_WITHIN_BBOX',
    'GEO_POINT', 'GEO_DISTANCE', 'GEO_WITHIN_BBOX', 'FTS_MATCH', 'FTS_RANK', 'FTS_SEARCH',
    'FTS_SNIPPET', 'BM25', 'VEC_FROM_JSON', 'VEC_SEARCH', 'VEC_COSINE_SIMILARITY',
    'VEC_DISTANCE', 'RAG_CONTEXT', 'RAG_CONTEXT_FROM', 'RAG_HYBRID_SCORE', 'RAG_RANK_SCORE',
    'RECENCY_SCORE', 'HASH', 'URL_PARSE', 'YAML_GET', 'CALL', 'ROUND'
];
// Safe references to WASM-exported functions (set after init)
let wasmApi = {
    importFile: null,
    executeQuery: null,
    executeMulti: null,
    clearDatabase: null,
    dropTable: null,
    listTables: null,
    exportResults: null,
    getTableSchema: null,
    exportDatabase: null,
    importDatabase: null,
};

// Client-side pending tables (used when WASM not ready)
const pendingClientTables = {};

// Query history (newest first, max 50)
const MAX_HISTORY = 50;
let queryHistory = loadHistory();
let autocompleteState = {
    visible: false,
    items: [],
    activeIndex: 0,
    rangeStart: 0,
    rangeEnd: 0,
};
let resultViewState = {
    filterText: '',
    sortColumn: '',
    sortDirection: 'asc',
};
let editorSaveTimer = null;
let snapshotSaveTimer = null;
let applyingHashDemo = false;
let lastAppliedHash = '';

function escapeRegex(text) {
    return String(text).replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

const SQL_HIGHLIGHT_KEYWORDS = [...new Set(SQL_KEYWORDS)]
    .sort((left, right) => right.length - left.length)
    .map((keyword) => escapeRegex(keyword).replace(/\s+/g, '\\s+'))
    .join('|');

const SQL_HIGHLIGHT_PATTERN = new RegExp(
    `(--[^\\n]*|\/\\*[\\s\\S]*?\\*\/|'(?:''|[^'])*'|\\b(?:${SQL_HIGHLIGHT_KEYWORDS})\\b|\\b\\d+(?:\\.\\d+)?\\b)`,
    'gi'
);

function loadHistory() {
    const legacyKeys = ['tinySQL_history', 'tsql_history'];
    try {
        const current = localStorage.getItem(HISTORY_KEY);
        if (current) {
            const parsed = JSON.parse(current);
            return Array.isArray(parsed) ? parsed : [];
        }
        for (const key of legacyKeys) {
            const legacy = localStorage.getItem(key);
            if (!legacy) continue;
            const parsed = JSON.parse(legacy);
            if (Array.isArray(parsed)) {
                localStorage.setItem(HISTORY_KEY, JSON.stringify(parsed));
                localStorage.removeItem(key);
                return parsed;
            }
        }
    } catch (_) {
        // Keep empty history if storage is corrupted or blocked.
    }
    return [];
}

function storageGet(key) {
    try {
        return window.localStorage ? window.localStorage.getItem(key) : null;
    } catch (_) {
        return null;
    }
}

function storageSet(key, value) {
    try {
        if (!window.localStorage) return false;
        window.localStorage.setItem(key, value);
        return true;
    } catch (error) {
        console.warn('localStorage write failed:', error);
        updateStatus('Local persistence failed');
        return false;
    }
}

function storageRemove(key) {
    try {
        if (window.localStorage) window.localStorage.removeItem(key);
    } catch (_) {
        // Ignore blocked storage cleanup.
    }
}

function base64UrlEncode(text) {
    const bytes = new TextEncoder().encode(String(text || ''));
    let binary = '';
    bytes.forEach((byte) => { binary += String.fromCharCode(byte); });
    return btoa(binary).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/g, '');
}

function base64UrlDecode(encoded) {
    const normalized = String(encoded || '').replace(/-/g, '+').replace(/_/g, '/');
    const padded = normalized + '='.repeat((4 - normalized.length % 4) % 4);
    const binary = atob(padded);
    const bytes = Uint8Array.from(binary, (char) => char.charCodeAt(0));
    return new TextDecoder().decode(bytes);
}

function encodeDemoHash(payload) {
    return `${DEMO_HASH_PREFIX}${base64UrlEncode(JSON.stringify(payload))}`;
}

function decodeDemoHash(hash = window.location.hash) {
    const raw = String(hash || '').replace(/^#/, '');
    if (!raw.startsWith(DEMO_HASH_PREFIX)) {
        return null;
    }
    try {
        const payload = JSON.parse(base64UrlDecode(raw.slice(DEMO_HASH_PREFIX.length)));
        if (!payload || payload.kind !== 'tinysql-demo' || !Array.isArray(payload.tables)) {
            return null;
        }
        return payload;
    } catch (error) {
        console.warn('Invalid tinySQL demo hash:', error);
        return null;
    }
}

async function instantiateWasm(go) {
    const wasmURL = 'query_files.wasm';
    if (WebAssembly.instantiateStreaming) {
        try {
            return await WebAssembly.instantiateStreaming(fetch(wasmURL), go.importObject);
        } catch (error) {
            console.warn('instantiateStreaming failed, falling back to ArrayBuffer:', error);
        }
    }
    const response = await fetch(wasmURL);
    const bytes = await response.arrayBuffer();
    return WebAssembly.instantiate(bytes, go.importObject);
}

function saveEditorState() {
    const editor = document.getElementById('queryEditor');
    if (editor) {
        storageSet(EDITOR_STATE_KEY, editor.value);
    }
}

function scheduleEditorStateSave() {
    window.clearTimeout(editorSaveTimer);
    editorSaveTimer = window.setTimeout(saveEditorState, 150);
}

function restoreEditorState() {
    const editor = document.getElementById('queryEditor');
    const value = storageGet(EDITOR_STATE_KEY);
    if (editor && value !== null && editor.value.trim() === '') {
        editor.value = value;
        syncEditorHighlight();
    }
}

function saveDatabaseSnapshotNow() {
    if (!wasmReady || typeof wasmApi.exportDatabase !== 'function') {
        return false;
    }
    try {
        const result = wasmApi.exportDatabase();
        if (!result || !result.success || typeof result.data !== 'string') {
            console.warn('Database snapshot export failed:', result?.error || result);
            return false;
        }
        const payload = {
            version: 1,
            savedAt: new Date().toISOString(),
            sizeBytes: result.sizeBytes || 0,
            data: result.data,
        };
        return storageSet(DB_SNAPSHOT_KEY, JSON.stringify(payload));
    } catch (error) {
        console.warn('Database snapshot export failed:', error);
        return false;
    }
}

function scheduleDatabaseSnapshotSave(delay = 250) {
    window.clearTimeout(snapshotSaveTimer);
    snapshotSaveTimer = window.setTimeout(saveDatabaseSnapshotNow, delay);
}

function restoreDatabaseSnapshot() {
    if (typeof wasmApi.importDatabase !== 'function') {
        return false;
    }
    const raw = storageGet(DB_SNAPSHOT_KEY);
    if (!raw) {
        return false;
    }
    try {
        let snapshot = raw;
        try {
            const payload = JSON.parse(raw);
            if (payload && typeof payload.data === 'string') {
                snapshot = payload.data;
            }
        } catch (_) {
            // Backward-compatible path for raw base64 snapshots.
        }
        const result = wasmApi.importDatabase(snapshot);
        if (!result || !result.success) {
            storageRemove(DB_SNAPSHOT_KEY);
            updateStatus(`Saved database could not be restored: ${result?.error || 'unknown error'}`);
            return false;
        }
        updateStatus('Restored local database snapshot');
        return true;
    } catch (error) {
        storageRemove(DB_SNAPSHOT_KEY);
        updateStatus(`Saved database could not be restored: ${error.message}`);
        return false;
    }
}

function sqlMayMutate(sql) {
    const stripped = String(sql || '')
        .replace(/--[^\n]*/g, ' ')
        .replace(/\/\*[\s\S]*?\*\//g, ' ')
        .replace(/'(?:''|[^'])*'/g, "''")
        .trim();
    if (!stripped) return false;
    return stripped.split(';').some((statement) => {
        const first = statement.trim().split(/\s+/)[0]?.toUpperCase();
        return first && !['SELECT', 'WITH', 'EXPLAIN', 'SHOW', 'DESCRIBE', 'PRAGMA'].includes(first);
    });
}

// Initialize WASM
async function initWasm() {
    const go = new Go();
    
    try {
        const result = await instantiateWasm(go);
        
        go.run(result.instance || result);
        wasmReady = true;
        console.log("WASM initialized successfully");
        
        // Capture WASM API references safely
        wasmApi.importFile = window.importFile;
        wasmApi.executeQuery = window.executeQuery;
        wasmApi.executeMulti = window.executeMulti;
        wasmApi.clearDatabase = window.clearDatabase;
        wasmApi.dropTable = window.dropTable;
        wasmApi.listTables = window.listTables;
        wasmApi.exportResults = window.exportResults;
        wasmApi.getTableSchema = window.getTableSchema;
        wasmApi.exportDatabase = window.exportDatabase;
        wasmApi.importDatabase = window.importDatabase;

        console.log("Available WASM functions:", Object.fromEntries(
            Object.entries(wasmApi).map(([k,v]) => [k, typeof v])
        ));
        
        updateStatus("Ready");
        document.querySelector('.status-indicator').classList.add('ready');
        document.getElementById('executeBtn').disabled = false;
        const hashDemoPayload = decodeDemoHash();
        if (!hashDemoPayload) {
            restoreDatabaseSnapshot();
        }
        // If any tables were registered client-side before WASM was ready,
        // import them now into the WASM-backed database so queries will work.
        if (Object.keys(pendingClientTables).length > 0) {
            console.log('Importing pending client tables into WASM:', Object.keys(pendingClientTables));
            for (const [tableName, rows] of Object.entries(pendingClientTables)) {
                try {
                    const jsonContent = JSON.stringify(rows);
                    const result = wasmApi.importFile(`${tableName}.json`, jsonContent, tableName);
                    console.log(`Imported pending table ${tableName} to WASM:`, result);
                    // If successful, ensure table is present in currentTables
                    if (result && result.success) {
                        const tableInfo = {
                            name: tableName,
                            rowCount: result.rowsImported,
                            columns: Array.isArray(result.columns) ? result.columns.map(c => String(c)) : []
                        };
                        const existingIndex = currentTables.findIndex(t => t.name === tableName);
                        if (existingIndex >= 0) currentTables[existingIndex] = tableInfo;
                        else currentTables.push(tableInfo);
                    }
                } catch (err) {
                    console.error(`Failed to import pending table ${tableName}:`, err);
                }
            }
            renderTables();
            // Clear pending list now that we've attempted to import
            for (const k of Object.keys(pendingClientTables)) delete pendingClientTables[k];
            scheduleDatabaseSnapshotSave();
        }
        loadTables();
        if (hashDemoPayload) {
            await applyHashDemoPayload(hashDemoPayload);
            lastAppliedHash = window.location.hash || '';
        }
    } catch (err) {
        console.error("Failed to load WASM:", err);
        updateStatus("Failed to load WASM");
        document.querySelector('.status-indicator').classList.add('failed');
    }
}

// Demo data
function createSeededRandom(seed) {
    let state = seed >>> 0;
    return function nextRandom() {
        state = (state * 1664525 + 1013904223) >>> 0;
        return state / 4294967296;
    };
}

function pickValue(values, random) {
    return values[Math.floor(random() * values.length)];
}

function randomInt(random, min, max) {
    return Math.floor(random() * (max - min + 1)) + min;
}

function randomNumber(random, min, max, decimals = 2) {
    return Number((min + random() * (max - min)).toFixed(decimals));
}

function formatIsoDate(date) {
    return date.toISOString().slice(0, 10);
}

function formatIsoDateTime(date) {
    return date.toISOString().slice(0, 19).replace('T', ' ');
}

function generateLargeSalesData(rowCount) {
    const random = createSeededRandom(1337);
    const catalog = [
        { product: 'Widget A', category: 'Widgets', minPrice: 24, maxPrice: 36 },
        { product: 'Widget B', category: 'Widgets', minPrice: 39, maxPrice: 58 },
        { product: 'Widget C', category: 'Widgets', minPrice: 68, maxPrice: 92 },
        { product: 'Sensor Hub', category: 'Electronics', minPrice: 120, maxPrice: 185 },
        { product: 'Analytics Suite', category: 'Software', minPrice: 210, maxPrice: 360 },
        { product: 'Edge Gateway', category: 'Infrastructure', minPrice: 440, maxPrice: 680 }
    ];
    const customerPrefixes = ['Acme', 'Northwind', 'BluePeak', 'Signal', 'Vertex', 'Bright', 'Nimbus', 'Evergreen', 'Cobalt', 'Atlas'];
    const customerSuffixes = ['Retail', 'Logistics', 'Systems', 'Works', 'Labs', 'Partners', 'Industries', 'Solutions', 'Stores', 'Networks'];
    const segments = ['SMB', 'Mid-Market', 'Enterprise', 'Public Sector'];
    const regions = ['North', 'South', 'East', 'West', 'Central'];
    const channels = ['Direct', 'Partner', 'Online', 'Inside Sales'];
    const statuses = ['Delivered', 'Delivered', 'Delivered', 'Shipped', 'Processing', 'Backorder'];
    const priorities = ['Low', 'Normal', 'High', 'Urgent'];
    const salesReps = ['A. Cole', 'B. Rivera', 'C. Shah', 'D. Fischer', 'E. Novak', 'F. Silva'];
    const baseDate = Date.UTC(2024, 0, 1);
    const rows = [];

    for (let index = 0; index < rowCount; index += 1) {
        const item = pickValue(catalog, random);
        const quantity = randomInt(random, 1, 120);
        const unitPrice = randomNumber(random, item.minPrice, item.maxPrice);
        const discountPct = randomInt(random, 0, 18);
        const grossTotal = Number((quantity * unitPrice).toFixed(2));
        const orderTotal = Number((grossTotal * (1 - discountPct / 100)).toFixed(2));
        const customerNumber = 1000 + randomInt(random, 0, 899);
        const orderDate = new Date(baseDate + randomInt(random, 0, 210) * 86400000);

        rows.push({
            order_id: 200000 + index,
            customer_id: `CUST-${customerNumber}`,
            customer_name: `${pickValue(customerPrefixes, random)} ${pickValue(customerSuffixes, random)}`,
            segment: pickValue(segments, random),
            region: pickValue(regions, random),
            channel: pickValue(channels, random),
            product: item.product,
            category: item.category,
            quantity,
            unit_price: unitPrice,
            discount_pct: discountPct,
            gross_total: grossTotal,
            order_total: orderTotal,
            status: pickValue(statuses, random),
            priority: pickValue(priorities, random),
            sales_rep: pickValue(salesReps, random),
            order_date: formatIsoDate(orderDate)
        });
    }

    return rows;
}

function generateLargeLogisticsData(salesRows) {
    const random = createSeededRandom(2024);
    const carriers = ['FastShip Express', 'QuickMove Logistics', 'RapidTransit Co', 'Northern Freight', 'CargoStream'];
    const warehouses = ['New York', 'Chicago', 'Dallas', 'Seattle', 'Rotterdam'];
    const serviceLevels = ['Standard', 'Priority', 'Two-Day', 'Economy'];
    const statuses = ['Delivered', 'Delivered', 'Delivered', 'In Transit', 'Processing', 'Delayed'];

    return salesRows.map((sale, index) => {
        const dispatchDelay = randomInt(random, 0, 3);
        const deliveryDays = randomInt(random, 1, 8);
        const dispatchDate = new Date(Date.parse(`${sale.order_date}T08:00:00Z`) + dispatchDelay * 86400000);
        const status = pickValue(statuses, random);
        const deliveryDate = status === 'Delivered'
            ? formatIsoDate(new Date(dispatchDate.getTime() + deliveryDays * 86400000))
            : null;

        return {
            shipment_id: `SHP-${sale.order_id}`,
            order_id: sale.order_id,
            customer_id: sale.customer_id,
            warehouse: pickValue(warehouses, random),
            carrier: pickValue(carriers, random),
            service_level: pickValue(serviceLevels, random),
            origin_region: pickValue(['North', 'South', 'East', 'West', 'Central'], random),
            destination_region: sale.region,
            weight_kg: randomNumber(random, 5, 380, 1),
            distance_km: randomInt(random, 120, 5200),
            shipping_cost: randomNumber(random, 35, 920),
            delivery_days: status === 'Delivered' ? deliveryDays : null,
            status,
            dispatch_date: formatIsoDate(dispatchDate),
            delivery_date: deliveryDate,
            batch_id: `BATCH-${100 + (index % 48)}`
        };
    });
}

function generateLargeWebEventsData(salesRows, rowCount) {
    const random = createSeededRandom(4242);
    const eventTypes = ['page_view', 'product_view', 'search', 'add_to_cart', 'checkout_start', 'purchase', 'support_chat'];
    const pages = ['/home', '/pricing', '/catalog', '/products/widget-a', '/products/widget-b', '/checkout', '/support'];
    const devices = ['desktop', 'mobile', 'tablet'];
    const countries = ['US', 'DE', 'FR', 'UK', 'NL', 'SE'];
    const acquisitionChannels = ['organic', 'paid', 'email', 'partner', 'direct'];
    const baseDate = Date.UTC(2024, 0, 1);
    const rows = [];

    for (let index = 0; index < rowCount; index += 1) {
        const sale = salesRows[randomInt(random, 0, salesRows.length - 1)];
        const eventType = pickValue(eventTypes, random);
        const eventDate = new Date(
            baseDate + randomInt(random, 0, 240) * 86400000 + randomInt(random, 0, 1439) * 60000
        );
        const revenueImpact = eventType === 'purchase'
            ? sale.order_total
            : Number((sale.order_total * random() * 0.08).toFixed(2));

        rows.push({
            event_id: `EVT-${500000 + index}`,
            session_id: `SES-${200000 + randomInt(random, 0, 90000)}`,
            customer_id: sale.customer_id,
            order_id: eventType === 'purchase' ? sale.order_id : null,
            event_type: eventType,
            page: pickValue(pages, random),
            device: pickValue(devices, random),
            region: sale.region,
            country: pickValue(countries, random),
            acquisition_channel: pickValue(acquisitionChannels, random),
            event_date: formatIsoDate(eventDate),
            event_timestamp: formatIsoDateTime(eventDate),
            duration_seconds: randomInt(random, 5, 1800),
            revenue_impact: revenueImpact,
            converted: eventType === 'purchase' ? 1 : (random() < 0.06 ? 1 : 0)
        });
    }

    return rows;
}

let generatedDemoTables = null;

function getGeneratedDemoTables() {
    if (generatedDemoTables) {
        return generatedDemoTables;
    }

    const salesLarge = generateLargeSalesData(5000);
    generatedDemoTables = {
        sales_large: salesLarge,
        logistics_large: generateLargeLogisticsData(salesLarge),
        web_events_large: generateLargeWebEventsData(salesLarge, 10000)
    };

    return generatedDemoTables;
}

const DEMO_GEOJSON = {
    type: 'FeatureCollection',
    features: [
        {
            type: 'Feature',
            properties: { name: 'Berlin Hub', city: 'Berlin', role: 'warehouse' },
            geometry: { type: 'Point', coordinates: [13.4050, 52.5200] }
        },
        {
            type: 'Feature',
            properties: { name: 'Munich Depot', city: 'Munich', role: 'warehouse' },
            geometry: { type: 'Point', coordinates: [11.5755, 48.1372] }
        },
        {
            type: 'Feature',
            properties: { name: 'Zurich Crossdock', city: 'Zurich', role: 'crossdock' },
            geometry: { type: 'Point', coordinates: [8.5417, 47.3769] }
        },
        {
            type: 'Feature',
            properties: { name: 'Hamburg Port', city: 'Hamburg', role: 'port' },
            geometry: { type: 'Point', coordinates: [9.9937, 53.5511] }
        },
        {
            type: 'Feature',
            properties: { name: 'Vienna Terminal', city: 'Vienna', role: 'terminal' },
            geometry: { type: 'Point', coordinates: [16.3738, 48.2082] }
        },
        {
            type: 'Feature',
            properties: { name: 'Amsterdam Gateway', city: 'Amsterdam', role: 'gateway' },
            geometry: { type: 'Point', coordinates: [4.9041, 52.3676] }
        }
    ]
};

const DEMO_ROUTING_GRAPH = [
    JSON.stringify({ type: 'node', id: 'berlin', lat: 52.5200, lon: 13.4050, properties: { city: 'Berlin' } }),
    JSON.stringify({ type: 'node', id: 'munich', lat: 48.1372, lon: 11.5755, properties: { city: 'Munich' } }),
    JSON.stringify({ type: 'node', id: 'zurich', lat: 47.3769, lon: 8.5417, properties: { city: 'Zurich' } }),
    JSON.stringify({ type: 'node', id: 'hamburg', lat: 53.5511, lon: 9.9937, properties: { city: 'Hamburg' } }),
    JSON.stringify({ type: 'node', id: 'vienna', lat: 48.2082, lon: 16.3738, properties: { city: 'Vienna' } }),
    JSON.stringify({
        type: 'edge',
        id: 'berlin-munich',
        source: 'berlin',
        target: 'munich',
        distance: 585000,
        duration: 21600,
        mode: 'road',
        geometry: { type: 'LineString', coordinates: [[13.4050, 52.5200], [11.5755, 48.1372]] }
    }),
    JSON.stringify({
        type: 'edge',
        id: 'munich-zurich',
        source: 'munich',
        target: 'zurich',
        distance: 315000,
        duration: 12600,
        mode: 'road',
        geometry: { type: 'LineString', coordinates: [[11.5755, 48.1372], [8.5417, 47.3769]] }
    }),
    JSON.stringify({
        type: 'edge',
        id: 'hamburg-berlin',
        source: 'hamburg',
        target: 'berlin',
        distance: 289000,
        duration: 10800,
        mode: 'rail',
        geometry: { type: 'LineString', coordinates: [[9.9937, 53.5511], [13.4050, 52.5200]] }
    }),
    JSON.stringify({
        type: 'edge',
        id: 'munich-vienna',
        source: 'munich',
        target: 'vienna',
        distance: 435000,
        duration: 16200,
        mode: 'road',
        geometry: { type: 'LineString', coordinates: [[11.5755, 48.1372], [16.3738, 48.2082]] }
    })
].join('\n');

const DEMO_GEO_ZONES = [
    { zone_name: 'DACH Core', min_lon: 5.5, min_lat: 45.5, max_lon: 17.5, max_lat: 55.2 },
    { zone_name: 'Northern Corridor', min_lon: 4.0, min_lat: 52.0, max_lon: 14.5, max_lat: 54.5 },
    { zone_name: 'Alpine Reach', min_lon: 6.5, min_lat: 46.0, max_lon: 17.5, max_lat: 49.5 },
    { zone_name: 'Benelux Access', min_lon: 3.0, min_lat: 50.5, max_lon: 7.5, max_lat: 53.8 }
];

const DEMO_YAML = `- service: api
  region: eu-central
  active: true
  replicas: 3
- service: tiles
  region: global
  active: true
  replicas: 6
- service: batch
  region: us-east
  active: false
  replicas: 1
`;

const DEMO_AI_DOCS = [
    {
        id: 1,
        title: 'Vector Search',
        category: 'ai',
        content: 'Vector search finds semantically similar records with embeddings and nearest-neighbor ranking.',
        embedding: '[1.0, 0.0, 0.0]'
    },
    {
        id: 2,
        title: 'Full Text Search',
        category: 'search',
        content: 'Full text search ranks documents by matching query terms, phrases, and boolean expressions.',
        embedding: '[0.0, 1.0, 0.0]'
    },
    {
        id: 3,
        title: 'Geo Analytics',
        category: 'geo',
        content: 'Geo analytics combines coordinates, distances, bounding boxes, and routing graph data.',
        embedding: '[0.0, 0.0, 1.0]'
    },
    {
        id: 4,
        title: 'Hybrid Retrieval',
        category: 'ai',
        content: 'Hybrid retrieval combines full text ranking with vector similarity for RAG applications.',
        embedding: '[0.8, 0.2, 0.0]'
    }
];

const DEMO_RAG_CHUNKS = [
    { doc_id: 'tinySQL', chunk_index: 0, chunk_text: 'tinySQL added browser-ready file analytics, query history, snapshots, and shareable URL hash demos.', quality: 0.78, created_at: '2026-07-08 10:00:00', embedding: '[0.9, 0.2, 0.0]' },
    { doc_id: 'tinySQL', chunk_index: 1, chunk_text: 'Geodata imports now cover GeoJSON, KML ExtendedData and MultiGeometry, OSM XML, routing graph NDJSON, Shapefile ZIP, and MBTiles metadata.', quality: 0.94, created_at: '2026-07-08 11:00:00', embedding: '[1.0, 0.1, 0.1]' },
    { doc_id: 'tinySQL', chunk_index: 2, chunk_text: 'RAG helpers combine FTS snippets, vector search, context expansion, recency scoring, and quality-weighted hybrid ranking.', quality: 0.96, created_at: '2026-07-08 12:00:00', embedding: '[0.8, 0.6, 0.1]' },
    { doc_id: 'tinySQL', chunk_index: 3, chunk_text: 'SQL analytics gained CTE views, materialized views, PIVOT, RETURNING, EXPLAIN, SQLite-compatible PRAGMA metadata, and richer sys catalog tables.', quality: 0.91, created_at: '2026-07-08 13:00:00', embedding: '[0.4, 0.9, 0.2]' },
    { doc_id: 'ops', chunk_index: 0, chunk_text: 'Operational work added RBAC, audit logging, storage and WAL improvements, tinysqld HTTP APIs, MCP server tools, and tinyORM examples.', quality: 0.86, created_at: '2026-07-08 14:00:00', embedding: '[0.2, 0.4, 1.0]' }
];

const DEMO_RELEASE_FEATURES = [
    { area: 'Geodata', feature: 'GeoJSON importer', added: '2026-07-08', browser_demo: 'Direct upload/import and ST_* SQL examples' },
    { area: 'Geodata', feature: 'KML ExtendedData, SchemaData, MultiGeometry, altitude', added: '2026-07-08', browser_demo: 'Direct .kml import' },
    { area: 'Geodata', feature: 'OSM XML nodes, ways, relations, refs, geometry', added: '2026-07-08', browser_demo: 'Direct .osm/.osm.xml import' },
    { area: 'Geodata', feature: 'Routing graph JSON/CSV/NDJSON with node and edge tables', added: '2026-07-08', browser_demo: 'Direct .rg and .graph.json import' },
    { area: 'Geodata', feature: 'Shapefile ZIP and MBTiles metadata imports', added: '2026-07-08', browser_demo: 'Go/CLI/server-side; documented in browser feature matrix' },
    { area: 'Search/RAG', feature: 'FTS snippets, BM25 ranking, vector indexes, RAG context, hybrid scoring', added: '2026-05-10 to 2026-07-08', browser_demo: 'Direct SQL over ai_docs and rag_chunks' },
    { area: 'Analytics SQL', feature: 'CTE views, materialized views, PIVOT, RETURNING, EXPLAIN', added: '2026-06-21 to 2026-07-08', browser_demo: 'Direct multi-statement SQL recipes' },
    { area: 'Catalog', feature: 'sys.* metadata, dependencies, functions, procedures, SQLite-compatible PRAGMAs', added: '2026-06-21 to 2026-07-08', browser_demo: 'Direct catalog queries' },
    { area: 'Security/Ops', feature: 'RBAC, audit logs, encryption, WAL/storage, tinysqld, MCP server', added: '2026-05-14 to 2026-07-05', browser_demo: 'Feature matrix and metadata queries; server-side examples in Go tools' },
    { area: 'Developer UX', feature: 'tinyORM, public importer/resultutil/sqlutil/jobs/standards packages, gh-pages workflow', added: '2026-07-05 to 2026-07-08', browser_demo: 'Documented and linked from demo README' }
];

const SHAREABLE_DEMOS = {
    release: {
        title: 'What changed recently',
        description: 'A compact two-month feature matrix with direct browser coverage and server-side-only notes.',
        icon: '🚀',
        tables: ['release_features'],
        autoRun: true,
        query: `-- Last two months: feature areas and what this WASM demo can show\nSELECT area, feature, browser_demo\nFROM release_features\nORDER BY area, feature`
    },
    geo: {
        title: 'Geodata lab',
        description: 'GeoJSON points, routing graph nodes/edges, bounding boxes, radius filters, and distance calculations.',
        icon: '🗺️',
        tables: ['places_geo', 'geo_zones', 'routes_rg'],
        autoRun: true,
        query: `-- Shareable Geo demo: zones + hubs\nSELECT z.zone_name, p.city, p.role,\n       ROUND(ST_DISTANCE(p.geometry, ST_MakePoint(13.4050, 52.5200)) / 1000, 1) AS km_from_berlin\nFROM places_geo p\nJOIN geo_zones z ON ST_WITHIN_BBOX(p.geometry, z.min_lon, z.min_lat, z.max_lon, z.max_lat)\nORDER BY z.zone_name, km_from_berlin`
    },
    rag: {
        title: 'FTS + vector retrieval',
        description: 'A compact RAG-style corpus with full-text ranking and vector similarity running in the browser.',
        icon: '🧠',
        tables: ['ai_docs'],
        autoRun: true,
        query: `-- Shareable RAG demo: full-text + vector score\nSELECT title, category,\n       FTS_RANK(content, 'vector OR search') AS text_rank,\n       VEC_COSINE_SIMILARITY(embedding, VEC_FROM_JSON('[1.0, 0.0, 0.0]')) AS vector_similarity\nFROM ai_docs\nWHERE FTS_MATCH(content, 'vector OR search')\nORDER BY vector_similarity DESC`
    },
    ragcontext: {
        title: 'RAG context expansion',
        description: 'Vector top-k retrieval expanded into neighboring chunks with quality and recency-aware ranking.',
        icon: '🔗',
        tables: ['rag_chunks'],
        autoRun: true,
        query: `-- Shareable RAG context demo: vector hit + surrounding chunks\nWITH topk AS (\n    SELECT doc_id, chunk_index, _vec_rank\n    FROM VEC_SEARCH('rag_chunks', 'embedding', VEC_FROM_JSON('[0.8, 0.6, 0.1]'), 1, 'cosine')\n)\nSELECT doc_id, chunk_index, chunk_text, _hit_rank, _context_offset\nFROM RAG_CONTEXT_FROM('rag_chunks', 'doc_id', 'chunk_index', 'topk', 'doc_id', 'chunk_index', 1)\nORDER BY _context_rank`
    },
    sqlfeatures: {
        title: 'Views, PIVOT, RETURNING',
        description: 'Recent analytics SQL features in one repeatable multi-statement browser recipe.',
        icon: '🧮',
        tables: ['sales'],
        autoRun: true,
        query: `-- Shareable SQL feature demo: views, materialized views, RETURNING\nDROP MATERIALIZED VIEW IF EXISTS demo_revenue_mv;\nDROP VIEW IF EXISTS demo_paid_orders;\nCREATE VIEW demo_paid_orders AS\nSELECT customer_name, region, product, quantity * unit_price AS revenue\nFROM sales\nWHERE status = 'Delivered';\nCREATE MATERIALIZED VIEW demo_revenue_mv AS\nSELECT region, SUM(revenue) AS revenue\nFROM demo_paid_orders\nGROUP BY region\nWITH DATA;\nINSERT INTO sales VALUES (1011, 'Acme Corp', 'Widget D', 10, 120.00, '2024-03-01', 'North', 'Delivered') RETURNING order_id, customer_name, quantity * unit_price AS returned_total;\nREFRESH MATERIALIZED VIEW demo_revenue_mv;\nSELECT region, revenue\nFROM demo_revenue_mv\nORDER BY revenue DESC`
    },
    catalog: {
        title: 'sys catalog introspection',
        description: 'Inspect loaded tables, registered SQL functions, stored procedures, and runtime status from SQL.',
        icon: '🧭',
        tables: ['release_features', 'ai_docs'],
        autoRun: true,
        query: `-- Shareable catalog demo: tinySQL can query its own metadata\nSELECT 'tables' AS kind, name AS item, rows AS detail\nFROM sys.tables\nUNION ALL\nSELECT 'procedures' AS kind, name AS item, storage AS detail\nFROM sys.procedures\nUNION ALL\nSELECT 'status' AS kind, key AS item, value AS detail\nFROM sys.status\nWHERE key IN ('go_version', 'goroutines')\nORDER BY kind, item`
    },
    files: {
        title: 'Multi-format file analytics',
        description: 'JSON and YAML demo data imported as typed tables, then queried with ordinary SQL.',
        icon: '📁',
        tables: ['sales', 'settings_yaml'],
        autoRun: true,
        query: `-- Shareable file analytics demo\nSELECT service AS item, region AS segment, replicas AS metric\nFROM settings_yaml\nUNION ALL\nSELECT product AS item, region AS segment, quantity AS metric\nFROM sales\nORDER BY segment, item`
    },
    analytics: {
        title: 'Joins and reporting',
        description: 'A small sales/logistics model demonstrating joins, aggregation, calculated metrics, and export-ready results.',
        icon: '📊',
        tables: ['sales', 'logistics'],
        autoRun: true,
        query: `-- Shareable analytics demo: sales + logistics\nSELECT s.region,\n       l.carrier,\n       COUNT(*) AS orders,\n       SUM(s.quantity * s.unit_price) AS revenue,\n       AVG(l.shipping_cost) AS avg_shipping_cost\nFROM sales s\nJOIN logistics l ON s.order_id = l.order_id\nGROUP BY s.region, l.carrier\nORDER BY revenue DESC`
    }
};

function getDemoDefaultQuery(tableName) {
    const queries = {
        sales: `SELECT customer_name, product, quantity * unit_price AS total_value\nFROM sales\nORDER BY total_value DESC\nLIMIT 10`,
        logistics: `SELECT carrier, COUNT(*) AS shipment_count, AVG(shipping_cost) AS avg_cost\nFROM logistics\nGROUP BY carrier\nORDER BY shipment_count DESC`,
        sales_large: `SELECT region, channel, COUNT(*) AS orders, SUM(order_total) AS revenue\nFROM sales_large\nGROUP BY region, channel\nORDER BY revenue DESC`,
        logistics_large: `SELECT carrier, service_level, COUNT(*) AS shipments, AVG(delivery_days) AS avg_delivery_days\nFROM logistics_large\nGROUP BY carrier, service_level\nORDER BY shipments DESC`,
        web_events_large: `SELECT event_date, device, COUNT(*) AS events, SUM(revenue_impact) AS influenced_revenue\nFROM web_events_large\nGROUP BY event_date, device\nORDER BY event_date DESC`,
        places_geo: `SELECT name, city, role,\n       ST_X(geometry) AS lon,\n       ST_Y(geometry) AS lat,\n       ST_DISTANCE(geometry, ST_MakePoint(13.4050, 52.5200)) AS meters_from_berlin\nFROM places_geo\nORDER BY meters_from_berlin`,
        routes_rg: `SELECT edge_id, source, target, distance, duration, mode\nFROM routes_rg\nORDER BY distance`,
        geo_zones: `SELECT z.zone_name, p.city, p.role\nFROM places_geo p\nJOIN geo_zones z ON ST_WITHIN_BBOX(p.geometry, z.min_lon, z.min_lat, z.max_lon, z.max_lat)\nORDER BY z.zone_name, p.city`,
        settings_yaml: `SELECT service, region, active, replicas\nFROM settings_yaml\nORDER BY service`,
        ai_docs: `SELECT title, category,\n       FTS_RANK(content, 'vector OR search') AS text_rank,\n       VEC_COSINE_SIMILARITY(embedding, VEC_FROM_JSON('[1.0, 0.0, 0.0]')) AS vector_similarity\nFROM ai_docs\nWHERE FTS_MATCH(content, 'vector OR search')\nORDER BY vector_similarity DESC`,
        rag_chunks: `WITH topk AS (\n    SELECT doc_id, chunk_index, _vec_rank\n    FROM VEC_SEARCH('rag_chunks', 'embedding', VEC_FROM_JSON('[0.8, 0.6, 0.1]'), 1, 'cosine')\n)\nSELECT doc_id, chunk_index, chunk_text, _hit_rank, _context_offset\nFROM RAG_CONTEXT_FROM('rag_chunks', 'doc_id', 'chunk_index', 'topk', 'doc_id', 'chunk_index', 1)\nORDER BY _context_rank`,
        release_features: `SELECT area, feature, browser_demo\nFROM release_features\nORDER BY area, feature`
    };

    return queries[tableName] || '';
}

function isRoutingGraphFile(fileName) {
    const lower = String(fileName || '').toLowerCase();
    return lower.endsWith('.rg') ||
        lower.endsWith('.routinggraph') ||
        lower.endsWith('.routing-graph') ||
        lower.endsWith('.routing_graph') ||
        lower.endsWith('.graph.json') ||
        lower.endsWith('.routinggraph.json') ||
        lower.endsWith('.routing-graph.json') ||
        lower.endsWith('.routing_graph.json');
}

function demoTablePayload(tableName) {
    const demo = DEMO_TABLES[tableName];
    if (!demo) {
        throw new Error(`Unknown demo table: ${tableName}`);
    }
    const data = typeof demo.getData === 'function' ? demo.getData() : demo.data;
    const fileName = demo.fileName || `${tableName}.json`;
    return {
        name: tableName,
        fileName,
        content: typeof data === 'string' ? data : JSON.stringify(data),
    };
}

function buildShareableDemoPayload(demoId) {
    const recipe = SHAREABLE_DEMOS[demoId];
    if (!recipe) {
        throw new Error(`Unknown shareable demo: ${demoId}`);
    }
    return {
        kind: 'tinysql-demo',
        version: 1,
        id: demoId,
        title: recipe.title,
        description: recipe.description,
        query: recipe.query,
        autoRun: recipe.autoRun === true,
        tables: recipe.tables.map(demoTablePayload),
    };
}

function getShareableDemoHash(demoId) {
    return encodeDemoHash(buildShareableDemoPayload(demoId));
}

function getShareableDemoURL(demoId) {
    const base = `${window.location.origin}${window.location.pathname}`;
    return `${base}#${getShareableDemoHash(demoId)}`;
}

function loadShareableDemo(demoId) {
    const hash = getShareableDemoHash(demoId);
    if (window.location.hash === `#${hash}`) {
        const payload = decodeDemoHash(`#${hash}`);
        if (payload) {
            applyHashDemoPayload(payload);
            lastAppliedHash = `#${hash}`;
        }
    } else {
        window.location.hash = hash;
    }
}

async function copyDemoLink(demoId) {
    const url = getShareableDemoURL(demoId);
    try {
        await navigator.clipboard.writeText(url);
        showToast('Demo link copied', 'success');
    } catch (_) {
        window.prompt('Copy demo link', url);
    }
}

function renderIntroPage() {
    const resultsContainer = document.getElementById('resultsContainer');
    if (!resultsContainer || decodeDemoHash()) {
        return;
    }
    const starterDemoIDs = ['files', 'analytics', 'geo', 'rag', 'sqlfeatures', 'catalog', 'release', 'ragcontext'];
    const cards = starterDemoIDs.map((id) => [id, SHAREABLE_DEMOS[id]]).map(([id, demo]) => `
        <div class="intro-card">
            <h3>${demo.icon} ${escapeHtml(demo.title)}</h3>
            <p>${escapeHtml(demo.description)}</p>
            <code>${escapeHtml(demo.query.split('\n').find(line => !line.startsWith('--')) || demo.query)}</code>
            <div class="intro-card-actions">
                <button onclick="loadShareableDemo('${id}')">Run demo</button>
                <button class="secondary" onclick="copyDemoLink('${id}')">Copy link</button>
            </div>
        </div>
    `).join('');

    resultsContainer.innerHTML = `
        <div class="intro-page">
            <section class="intro-hero">
                <div>
                    <div class="intro-kicker">tinySQL WebAssembly playground</div>
                    <h2>Explore your data locally — files, analytics, maps, and AI-ready search.</h2>
                    <p class="intro-copy">
                        Start with a file, a reporting workflow, or geodata. tinySQL runs as a static WASM app:
                        no account, no backend, and your local snapshot stays in this browser.
                    </p>
                    <div class="intro-actions">
                        <button onclick="showUploadDialog()">Upload a file</button>
                        <button onclick="loadShareableDemo('analytics')">Explore analytics</button>
                        <button onclick="loadShareableDemo('geo')">Open geodata lab</button>
                        <button class="secondary" onclick="loadShareableDemo('rag')">Try AI search</button>
                    </div>
                </div>
                <div class="intro-metrics">
                    <div class="intro-metric"><strong>Local-first</strong><span>No backend, no account, snapshot stays in the browser.</span></div>
                    <div class="intro-metric"><strong>Typed imports</strong><span>CSV, JSON, YAML, XML, Excel, GeoJSON, KML, OSM, routing graph.</span></div>
                    <div class="intro-metric"><strong>SQL-rich</strong><span>CTEs, views, PIVOT, windows, geospatial functions, FTS, vector search.</span></div>
                </div>
            </section>
            <section class="feature-strip">
                <div class="feature-pill"><strong>Geodata-ready</strong>Distance, radius, bbox and routing-graph examples.</div>
                <div class="feature-pill"><strong>AI-compatible</strong>Full-text, vector similarity, and optional RAG-style retrieval recipes.</div>
                <div class="feature-pill"><strong>Release-aware</strong>Recent tinySQL features are grouped into runnable recipes.</div>
                <div class="feature-pill"><strong>Shareable</strong>Demo data and SQL travel in the URL hash.</div>
                <div class="feature-pill"><strong>Exportable</strong>Copy or export query results as CSV, TSV, Markdown, JSON, XML.</div>
            </section>
            <section class="intro-grid">${cards}</section>
        </div>
    `;
}

const DEMO_TABLES = {
    sales: {
        name: 'sales',
        data: [
            { order_id: 1001, customer_name: 'Acme Corp', product: 'Widget A', quantity: 50, unit_price: 29.99, order_date: '2024-01-15', region: 'North', status: 'Delivered' },
            { order_id: 1002, customer_name: 'TechStart Inc', product: 'Widget B', quantity: 30, unit_price: 45.50, order_date: '2024-01-18', region: 'South', status: 'Delivered' },
            { order_id: 1003, customer_name: 'Global Solutions', product: 'Widget A', quantity: 100, unit_price: 29.99, order_date: '2024-01-20', region: 'East', status: 'Processing' },
            { order_id: 1004, customer_name: 'Innovate LLC', product: 'Widget C', quantity: 25, unit_price: 75.00, order_date: '2024-01-22', region: 'West', status: 'Shipped' },
            { order_id: 1005, customer_name: 'Acme Corp', product: 'Widget B', quantity: 60, unit_price: 45.50, order_date: '2024-01-25', region: 'North', status: 'Delivered' },
            { order_id: 1006, customer_name: 'DataTech Pro', product: 'Widget A', quantity: 40, unit_price: 29.99, order_date: '2024-02-01', region: 'South', status: 'Delivered' },
            { order_id: 1007, customer_name: 'SmartBiz Co', product: 'Widget C', quantity: 15, unit_price: 75.00, order_date: '2024-02-05', region: 'East', status: 'Processing' },
            { order_id: 1008, customer_name: 'Global Solutions', product: 'Widget B', quantity: 80, unit_price: 45.50, order_date: '2024-02-08', region: 'West', status: 'Shipped' },
            { order_id: 1009, customer_name: 'TechStart Inc', product: 'Widget A', quantity: 55, unit_price: 29.99, order_date: '2024-02-10', region: 'North', status: 'Delivered' },
            { order_id: 1010, customer_name: 'Innovate LLC', product: 'Widget C', quantity: 35, unit_price: 75.00, order_date: '2024-02-15', region: 'South', status: 'Delivered' }
        ]
    },
    logistics: {
        name: 'logistics',
        data: [
            { shipment_id: 'SHP-001', order_id: 1001, origin: 'New York', destination: 'Los Angeles', carrier: 'FastShip Express', weight_kg: 150, distance_km: 4500, shipping_cost: 450.00, dispatch_date: '2024-01-10', delivery_date: '2024-01-15', status: 'Delivered' },
            { shipment_id: 'SHP-002', order_id: 1002, origin: 'Chicago', destination: 'Miami', carrier: 'QuickMove Logistics', weight_kg: 200, distance_km: 2100, shipping_cost: 380.00, dispatch_date: '2024-01-12', delivery_date: '2024-01-16', status: 'Delivered' },
            { shipment_id: 'SHP-003', order_id: 1003, origin: 'Seattle', destination: 'Boston', carrier: 'FastShip Express', weight_kg: 120, distance_km: 4800, shipping_cost: 520.00, dispatch_date: '2024-01-15', delivery_date: '2024-01-20', status: 'In Transit' },
            { shipment_id: 'SHP-004', order_id: 1004, origin: 'Dallas', destination: 'Denver', carrier: 'RapidTransit Co', weight_kg: 180, distance_km: 1200, shipping_cost: 280.00, dispatch_date: '2024-01-18', delivery_date: '2024-01-21', status: 'Delivered' },
            { shipment_id: 'SHP-005', order_id: 1005, origin: 'Phoenix', destination: 'Portland', carrier: 'QuickMove Logistics', weight_kg: 95, distance_km: 2000, shipping_cost: 340.00, dispatch_date: '2024-01-20', delivery_date: '2024-01-24', status: 'In Transit' },
            { shipment_id: 'SHP-006', order_id: 1006, origin: 'Atlanta', destination: 'Houston', carrier: 'FastShip Express', weight_kg: 220, distance_km: 1100, shipping_cost: 310.00, dispatch_date: '2024-01-22', delivery_date: '2024-01-25', status: 'Delivered' },
            { shipment_id: 'SHP-007', order_id: 1007, origin: 'San Francisco', destination: 'Chicago', carrier: 'RapidTransit Co', weight_kg: 160, distance_km: 3400, shipping_cost: 420.00, dispatch_date: '2024-01-25', delivery_date: null, status: 'In Transit' },
            { shipment_id: 'SHP-008', order_id: 1008, origin: 'Boston', destination: 'Dallas', carrier: 'QuickMove Logistics', weight_kg: 140, distance_km: 2700, shipping_cost: 390.00, dispatch_date: '2024-02-01', delivery_date: null, status: 'Processing' },
            { shipment_id: 'SHP-009', order_id: 1009, origin: 'Los Angeles', destination: 'Seattle', carrier: 'FastShip Express', weight_kg: 175, distance_km: 1800, shipping_cost: 350.00, dispatch_date: '2024-02-03', delivery_date: null, status: 'Processing' },
            { shipment_id: 'SHP-010', order_id: 1010, origin: 'Miami', destination: 'Phoenix', carrier: 'RapidTransit Co', weight_kg: 210, distance_km: 3200, shipping_cost: 480.00, dispatch_date: '2024-02-05', delivery_date: null, status: 'In Transit' }
        ]
    },
    sales_large: {
        name: 'sales_large',
        getData: () => getGeneratedDemoTables().sales_large
    },
    logistics_large: {
        name: 'logistics_large',
        getData: () => getGeneratedDemoTables().logistics_large
    },
    web_events_large: {
        name: 'web_events_large',
        getData: () => getGeneratedDemoTables().web_events_large
    },
    places_geo: {
        name: 'places_geo',
        fileName: 'places.geojson',
        getData: () => DEMO_GEOJSON
    },
    routes_rg: {
        name: 'routes_rg',
        fileName: 'routes.rg',
        getData: () => DEMO_ROUTING_GRAPH
    },
    geo_zones: {
        name: 'geo_zones',
        fileName: 'geo_zones.json',
        getData: () => DEMO_GEO_ZONES
    },
    settings_yaml: {
        name: 'settings_yaml',
        fileName: 'settings.yaml',
        getData: () => DEMO_YAML
    },
    ai_docs: {
        name: 'ai_docs',
        fileName: 'ai_docs.json',
        getData: () => DEMO_AI_DOCS
    },
    rag_chunks: {
        name: 'rag_chunks',
        fileName: 'rag_chunks.json',
        getData: () => DEMO_RAG_CHUNKS
    },
    release_features: {
        name: 'release_features',
        fileName: 'release_features.json',
        getData: () => DEMO_RELEASE_FEATURES
    }
};

// Load demo table
async function loadDemoTable(tableName) {
    if (!DEMO_TABLES[tableName]) {
        alert(`Demo table "${tableName}" not found`);
        return;
    }

    const demo = DEMO_TABLES[tableName];
    const rows = typeof demo.getData === 'function' ? demo.getData() : demo.data;
    const jsonContent = JSON.stringify(rows);
    const fileContent = typeof rows === 'string' ? rows : jsonContent;
    const fileName = demo.fileName || `${tableName}.json`;
    const suggestedQuery = getDemoDefaultQuery(tableName);
    
    updateStatus(`Loading demo table: ${tableName}...`);

        if (wasmReady && typeof wasmApi.importFile === 'function') {
            try {
                const result = wasmApi.importFile(fileName, fileContent, tableName);
                if (result && result.success) {
                    const tableInfo = {
                        name: tableName,
                        rowCount: result.rowsImported,
                        columns: Array.isArray(result.columns) ? result.columns.map(c => String(c)) : []
                    };
                    const existingIndex = currentTables.findIndex(t => t.name === tableName);
                    if (existingIndex >= 0) {
                        currentTables[existingIndex] = tableInfo;
                    } else {
                        currentTables.push(tableInfo);
                    }
                    renderTables();
                    if (isRoutingGraphFile(fileName)) {
                        loadTables();
                    }
                    updateStatus(`Demo table "${tableName}" loaded: ${result.rowsImported} rows`);
                    if (Object.prototype.hasOwnProperty.call(DEMO_TABLES, tableName)) {
                        showDemoQueries();
                    }
                    const editor = document.getElementById('queryEditor');
                    if (suggestedQuery) {
                        editor.value = suggestedQuery;
                        syncEditorHighlight();
                        saveEditorState();
                    }
                    document.getElementById('executeBtn').disabled = false;
                    scheduleDatabaseSnapshotSave();
                } else {
                    alert(`Failed to load demo table: ${result?.error || 'Unknown error'}`);
                    updateStatus('Demo load failed');
                }
            } catch (err) {
                alert(`Error loading demo table: ${err.message}`);
                updateStatus('Demo load failed');
            }
        } else {
            if (Array.isArray(rows)) {
                registerClientTable(tableName, rows);
            }
            const editor = document.getElementById('queryEditor');
            if (suggestedQuery) {
                editor.value = suggestedQuery;
                syncEditorHighlight();
                saveEditorState();
            }
            updateStatus(`Registered demo table "${tableName}" locally. Queries will run once WASM is initialized.`);
        }

}

async function importDemoPayloadTable(table) {
    if (!table || !table.name || typeof table.content !== 'string') {
        throw new Error('Invalid demo table payload');
    }
    const fileName = table.fileName || `${table.name}.json`;
    const result = wasmApi.importFile(fileName, table.content, table.name);
    if (!result || !result.success) {
        throw new Error(result?.error || `Import failed for ${table.name}`);
    }
    return {
        name: table.name,
        rowCount: result.rowsImported,
        columns: Array.isArray(result.columns) ? result.columns.map(c => String(c)) : [],
    };
}

async function applyHashDemoPayload(payload) {
    if (!payload || applyingHashDemo || !wasmReady || typeof wasmApi.importFile !== 'function') {
        return false;
    }
    applyingHashDemo = true;
    try {
        updateStatus(`Loading shared demo: ${payload.title || payload.id || 'tinySQL'}...`);
        if (typeof wasmApi.clearDatabase === 'function') {
            wasmApi.clearDatabase();
        }
        currentTables = [];
        currentResults = null;
        for (const key of Object.keys(pendingClientTables)) {
            delete pendingClientTables[key];
        }

        for (const table of payload.tables) {
            const tableInfo = await importDemoPayloadTable(table);
            currentTables.push(tableInfo);
            if (isRoutingGraphFile(table.fileName)) {
                loadTables();
            }
        }

        renderTables();
        showDemoQueries();
        if (payload.query) {
            setQuery(payload.query);
        }
        scheduleDatabaseSnapshotSave();
        updateStatus(`Loaded shared demo: ${payload.title || payload.id || 'tinySQL'}`);

        if (payload.autoRun && payload.query) {
            await onExecuteClick();
        }
        return true;
    } catch (error) {
        updateStatus('Shared demo failed');
        showToast(`Shared demo failed: ${error.message}`, 'error');
        console.error('applyHashDemoPayload failed:', error);
        return false;
    } finally {
        applyingHashDemo = false;
    }
}

async function applyHashDemoFromLocation() {
    const hash = window.location.hash || '';
    if (!hash || hash === lastAppliedHash) {
        return false;
    }
    const payload = decodeDemoHash(hash);
    if (!payload) {
        return false;
    }
    lastAppliedHash = hash;
    return applyHashDemoPayload(payload);
}

async function loadDemoTables(tableNames, finalQuery, statusText) {
    const total = tableNames.length;
    for (const [index, tableName] of tableNames.entries()) {
        updateStatus(`Loading demo ${index + 1}/${total}: ${tableName}...`);
        await loadDemoTable(tableName);
        if (index < tableNames.length - 1) {
            await new Promise(resolve => setTimeout(resolve, 20));
        }
    }

    if (finalQuery) {
        setQuery(finalQuery);
    }
    showDemoQueries();
    scheduleDatabaseSnapshotSave();
    updateStatus(statusText || `Loaded ${total} demo table(s)`);
}

async function loadGeoDemos() {
    loadShareableDemo('geo');
}

// Load all demo tables
async function loadAllDemos() {
    const tableNames = ['sales', 'logistics', 'places_geo', 'geo_zones', 'routes_rg', 'settings_yaml', 'ai_docs', 'rag_chunks', 'release_features', 'sales_large', 'logistics_large', 'web_events_large'];
    await loadDemoTables(
        tableNames,
        `-- Large demo: revenue and fulfillment by region and carrier\nSELECT s.region,\n       l.carrier,\n       COUNT(*) AS orders,\n       SUM(s.order_total) AS total_revenue,\n       AVG(l.shipping_cost) AS avg_shipping_cost\nFROM sales_large s\nJOIN logistics_large l ON s.order_id = l.order_id\nGROUP BY s.region, l.carrier\nORDER BY total_revenue DESC`,
        'Loaded curated demos and generated large tables'
    );
}

// Load tables on startup
document.addEventListener('DOMContentLoaded', () => {
    setupDragDrop();
    renderHistory();
    setupEditorSyntaxHighlighting();
    restoreEditorState();
    enhanceDemoQueries();
    setupAccessibilityShortcuts();
    setupSqlAutocomplete();
    renderIntroPage();
    initWasm();
    
    // Setup demo buttons
    const loadAllDemosBtn = document.getElementById('loadAllDemosBtn');
    if (loadAllDemosBtn) {
        loadAllDemosBtn.addEventListener('click', async () => {
            loadAllDemosBtn.style.display = 'none';
            try {
                await loadAllDemos();
            } catch (e) {
                console.error('Error loading demos:', e);
            }
        });
    }

    const loadGeoDemosBtn = document.getElementById('loadGeoDemosBtn');
    if (loadGeoDemosBtn) {
        loadGeoDemosBtn.addEventListener('click', () => {
            loadGeoDemosBtn.disabled = true;
            try {
                loadGeoDemos();
            } catch (e) {
                console.error('Error loading geo demos:', e);
                updateStatus('Geo demo load failed');
            } finally {
                window.setTimeout(() => { loadGeoDemosBtn.disabled = false; }, 300);
            }
        });
    }
});

window.addEventListener('hashchange', () => {
    if (wasmReady) {
        applyHashDemoFromLocation();
    }
});

// Setup drag and drop
function setupDragDrop() {
    const uploadBtn = document.querySelector('.upload-btn');
    
    uploadBtn.addEventListener('dragover', (e) => {
        e.preventDefault();
        uploadBtn.classList.add('dragover');
    });
    
    uploadBtn.addEventListener('dragleave', () => {
        uploadBtn.classList.remove('dragover');
    });
    
    uploadBtn.addEventListener('drop', (e) => {
        e.preventDefault();
        uploadBtn.classList.remove('dragover');
        handleFiles(e.dataTransfer.files);
    });
}

function enhanceDemoQueries() {
    document.querySelectorAll('.example-query').forEach((item) => {
        if (item.dataset.a11yEnhanced === 'true') {
            return;
        }
        item.dataset.a11yEnhanced = 'true';
        item.dataset.demoGroup = inferDemoQueryGroup(item.textContent);
        item.setAttribute('role', 'button');
        item.setAttribute('tabindex', '0');
        item.setAttribute('aria-label', `Load demo query: ${item.textContent.trim()}`);
        item.addEventListener('keydown', (event) => {
            if (event.key === 'Enter' || event.key === ' ') {
                event.preventDefault();
                item.click();
            }
        });
    });
    updateDemoQueryVisibility();
}

function inferDemoQueryGroup(text) {
    const label = String(text || '').toLowerCase();
    if (label.includes('recent') || label.includes('release') || label.includes('catalog') ||
        label.includes('pragma') || label.includes('explain') || label.includes('pivot') ||
        label.includes('returning') || label.includes('view')) {
        return 'recent';
    }
    if (label.includes('geo') || label.includes('bbox') || label.includes('node') ||
        label.includes('route') || label.includes('distance') || label.includes('radius') ||
        label.includes('zone') || label.includes('munich')) {
        return 'geo';
    }
    if (label.includes('fts') || label.includes('vector') || label.includes('hybrid') ||
        label.includes('rag') || label.includes('procedure') || label.includes('yaml')) {
        return 'search';
    }
    return 'analytics';
}

const DEMO_QUERY_REQUIREMENTS = {
    '🚀 Recent Features': 'release_features',
    '📊 Sales by Region': 'sales',
    '🏆 Top Products': 'sales',
    '🚚 Carrier Analysis': 'logistics',
    '🔗 JOIN Example': 'sales logistics',
    '💰 CASE Statement': 'sales',
    '⏳ Pending Shipments': 'logistics',
    '📈 Window Function': 'sales',
    '🧮 SQL PIVOT': 'sales',
    '🧱 Views + RETURNING': 'sales',
    '🔍 EXPLAIN': 'sales logistics',
    '🧾 PRAGMA table_info': 'sales',
    '🧮 Large Sales Cube': 'sales_large',
    '🚛 Large Logistics': 'logistics_large',
    '🌐 Event Trends': 'web_events_large',
    '🗺️ GeoJSON Distance': 'places_geo',
    '📍 Bounding Box': 'places_geo',
    '🧭 Routing Graph': 'routes_rg',
    '📌 Nearby Nodes': 'routes_rg_nodes',
    '🧱 Geo Zones': 'places_geo geo_zones',
    '📏 Distance Matrix': 'places_geo',
    '🛣️ Route Geometry': 'routes_rg routes_rg_nodes',
    '🎯 Munich Radius': 'places_geo',
    '⚙️ YAML Import': 'settings_yaml',
    '🔎 FTS Search': 'ai_docs',
    '🧠 Vector Search': 'ai_docs',
    '🧩 Hybrid Retrieval': 'ai_docs',
    '🔗 RAG Context': 'rag_chunks',
    '✂️ FTS Snippet': 'ai_docs',
};

function demoQueryRequirements(item) {
    if (item.dataset.requires) {
        return item.dataset.requires;
    }
    return DEMO_QUERY_REQUIREMENTS[String(item.textContent || '').trim()] || '';
}

function setDemoQueryGroup(group) {
    let selectedGroup = group || 'all';
    const availableItems = [...document.querySelectorAll('.example-query:not(.demo-query-unavailable)')];
    if (selectedGroup !== 'all' && !availableItems.some((item) => item.dataset.demoGroup === selectedGroup)) {
        selectedGroup = 'all';
    }
    document.querySelectorAll('.demo-filter button').forEach((button) => {
        button.classList.toggle('active', button.dataset.demoFilter === selectedGroup);
        const hasMatchingQuery = selectedGroup === 'all' || availableItems.some((item) => item.dataset.demoGroup === button.dataset.demoFilter);
        button.classList.toggle('hidden', button.dataset.demoFilter !== 'all' && !hasMatchingQuery);
    });
    document.querySelectorAll('.example-query').forEach((item) => {
        const matches = !item.classList.contains('demo-query-unavailable') &&
            (selectedGroup === 'all' || item.dataset.demoGroup === selectedGroup);
        item.classList.toggle('hidden', !matches);
    });
}

function updateDemoQueryVisibility() {
    const loadedTables = new Set(currentTables.map((table) => String(table.name).toLowerCase()));
    let availableCount = 0;

    document.querySelectorAll('.example-query').forEach((item) => {
        const requiredTables = demoQueryRequirements(item)
            .split(/\s+/)
            .map((name) => name.trim().toLowerCase())
            .filter(Boolean);
        const canRun = requiredTables.every((name) => loadedTables.has(name));
        item.classList.toggle('demo-query-unavailable', !canRun);
        if (canRun) {
            availableCount++;
        }
    });

    const demos = document.getElementById('demoQueries');
    if (demos) {
        demos.classList.toggle('hidden', availableCount === 0);
    }
    const activeFilter = document.querySelector('.demo-filter button.active')?.dataset.demoFilter || 'all';
    setDemoQueryGroup(activeFilter);
}

function setupAccessibilityShortcuts() {
    const schemaPanel = document.getElementById('schemaPanel');
    if (schemaPanel) {
        schemaPanel.addEventListener('keydown', (event) => {
            if (event.key === 'Escape') {
                closeSchemaPanel();
            }
        });
    }
}

function setupEditorSyntaxHighlighting() {
    const editor = document.getElementById('queryEditor');
    const highlight = document.getElementById('sqlHighlight');
    if (!editor || !highlight) {
        return;
    }

    const refresh = () => syncEditorHighlight();
    editor.addEventListener('input', refresh);
    editor.addEventListener('input', scheduleEditorStateSave);
    editor.addEventListener('scroll', refresh);
    editor.addEventListener('keyup', refresh);

    // Sync highlighting when editor is resized (e.g. drag handle)
    if (typeof ResizeObserver !== 'undefined') {
        new ResizeObserver(refresh).observe(editor);
    }

    // Update line/column counter on cursor changes
    const updateLineCount = () => {
        const counter = document.getElementById('editorLineCount');
        if (!counter) return;
        const pos = editor.selectionStart ?? 0;
        const textBefore = editor.value.slice(0, pos);
        const line = textBefore.split('\n').length;
        const col = pos - textBefore.lastIndexOf('\n');
        const totalLines = editor.value.split('\n').length;
        counter.textContent = `Ln ${line}, Col ${col} \u2022 ${totalLines} line${totalLines !== 1 ? 's' : ''}`;
    };
    editor.addEventListener('input', updateLineCount);
    editor.addEventListener('click', updateLineCount);
    editor.addEventListener('keyup', updateLineCount);
    editor.addEventListener('focus', updateLineCount);
    updateLineCount();

    refresh();
}

function syncEditorHighlight() {
    const editor = document.getElementById('queryEditor');
    const highlight = document.getElementById('sqlHighlight');
    if (!editor || !highlight) {
        return;
    }

    highlight.innerHTML = renderSqlHighlight(editor.value);
    highlight.scrollTop = editor.scrollTop;
    highlight.scrollLeft = editor.scrollLeft;
}

function renderSqlHighlight(text) {
    const raw = String(text || '');
    if (!raw) {
        return '<span class="sql-token muted">Type SQL to see highlighting</span>';
    }

    let html = '';
    let lastIndex = 0;
    SQL_HIGHLIGHT_PATTERN.lastIndex = 0;

    let match;
    while ((match = SQL_HIGHLIGHT_PATTERN.exec(raw)) !== null) {
        if (match.index > lastIndex) {
            html += escapeHtml(raw.slice(lastIndex, match.index));
        }

        const token = match[0];
        let className = 'keyword';
        if (token.startsWith('--') || token.startsWith('/*')) {
            className = 'comment';
        } else if (token.startsWith("'")) {
            className = 'string';
        } else if (/^\d/.test(token)) {
            className = 'number';
        }

        html += `<span class="sql-token ${className}">${escapeHtml(token)}</span>`;
        lastIndex = match.index + token.length;
    }

    if (lastIndex < raw.length) {
        html += escapeHtml(raw.slice(lastIndex));
    }

    return html.replace(/\n$/u, '\n ');
}

function setupSqlAutocomplete() {
    const editor = document.getElementById('queryEditor');
    const panel = document.getElementById('autocompletePanel');
    if (!editor || !panel) {
        return;
    }

    editor.setAttribute('aria-autocomplete', 'list');
    editor.setAttribute('aria-controls', 'autocompletePanel');
    editor.setAttribute('aria-haspopup', 'listbox');

    const refresh = () => updateAutocompleteSuggestions();

    editor.addEventListener('input', refresh);
    editor.addEventListener('click', refresh);
    editor.addEventListener('focus', refresh);
    editor.addEventListener('blur', () => {
        setTimeout(() => hideAutocompletePanel(), 150);
    });

    panel.addEventListener('mousedown', (event) => {
        const option = event.target.closest('[data-autocomplete-index]');
        if (!option) {
            return;
        }
        event.preventDefault();
        acceptAutocompleteSuggestion(Number(option.dataset.autocompleteIndex));
    });
}

function getKnownTableNames() {
    const names = new Set();
    for (const table of currentTables) {
        if (table && table.name) {
            names.add(String(table.name));
        }
    }
    for (const table of window._virtualTables || []) {
        if (table && table.name) {
            names.add(String(table.name));
        }
    }
    for (const name of Object.keys(pendingClientTables)) {
        names.add(name);
    }
    return [...names];
}

function getKnownColumnNames() {
    const names = new Set();
    for (const table of currentTables) {
        if (!table) continue;
        for (const column of table.columns || []) {
            if (column) {
                names.add(String(column));
                names.add(`${table.name}.${column}`);
            }
        }
    }
    for (const [tableName, rows] of Object.entries(pendingClientTables)) {
        if (!Array.isArray(rows) || rows.length === 0) continue;
        for (const column of Object.keys(rows[0])) {
            names.add(String(column));
            names.add(`${tableName}.${column}`);
        }
    }
    return [...names];
}

function getAutocompleteContext(editor) {
    const cursor = editor.selectionStart ?? 0;
    const before = editor.value.slice(0, cursor);
    const match = before.match(/([A-Za-z0-9_.$"]+)$/);
    const token = match ? match[1] : '';
    return {
        token,
        rangeStart: cursor - token.length,
        rangeEnd: cursor,
    };
}

function buildAutocompleteSuggestions(token, includeAll = false) {
    const raw = String(token || '').replace(/"/g, '').trim();
    const items = [];
    const addItem = (label, detail, insertText = label) => {
        const key = `${label}||${detail}`.toLowerCase();
        if (!items.some(item => `${item.label}||${item.detail}`.toLowerCase() === key)) {
            items.push({ label, detail, insertText });
        }
    };

    if (!raw) {
        if (includeAll) {
            for (const keyword of SQL_KEYWORDS) {
                addItem(keyword, 'SQL keyword');
            }
            for (const tableName of getKnownTableNames()) {
                addItem(tableName, 'table');
            }
        }
        return items.slice(0, 20);
    }

    const upper = raw.toUpperCase();
    const dotIndex = raw.lastIndexOf('.');

    if (dotIndex > 0 && dotIndex < raw.length) {
        const tablePrefix = raw.slice(0, dotIndex).replace(/"/g, '').trim();
        const columnPrefix = raw.slice(dotIndex + 1).replace(/"/g, '').trim().toUpperCase();
        for (const columnName of getKnownColumnNames()) {
            const [tableName, column] = columnName.split('.');
            if (tableName === tablePrefix && column && column.toUpperCase().startsWith(columnPrefix)) {
                addItem(columnName, 'table.column');
            }
        }
        return items.slice(0, 20);
    }

    for (const keyword of SQL_KEYWORDS) {
        if (keyword.startsWith(upper)) {
            addItem(keyword, 'SQL keyword');
        }
    }

    for (const tableName of getKnownTableNames()) {
        if (tableName.toUpperCase().startsWith(upper)) {
            addItem(tableName, 'table');
        }
    }

    for (const columnName of getKnownColumnNames()) {
        if (columnName.toUpperCase().startsWith(upper)) {
            addItem(columnName, columnName.includes('.') ? 'table.column' : 'column');
        }
    }

    return items.slice(0, 20);
}

function updateAutocompleteSuggestions(includeAll = false) {
    const editor = document.getElementById('queryEditor');
    const panel = document.getElementById('autocompletePanel');
    if (!editor || !panel) {
        return;
    }

    const context = getAutocompleteContext(editor);
    const items = buildAutocompleteSuggestions(context.token, includeAll);

    autocompleteState.rangeStart = context.rangeStart;
    autocompleteState.rangeEnd = context.rangeEnd;
    autocompleteState.items = items;
    autocompleteState.activeIndex = Math.min(autocompleteState.activeIndex, Math.max(items.length - 1, 0));

    if (items.length === 0 || (!includeAll && context.token.length < 1)) {
        hideAutocompletePanel();
        return;
    }

    panel.innerHTML = items.map((item, index) => `
        <div class="autocomplete-item${index === autocompleteState.activeIndex ? ' active' : ''}"
             role="option"
             aria-selected="${index === autocompleteState.activeIndex ? 'true' : 'false'}"
             data-autocomplete-index="${index}">
            <strong>${escapeHtml(item.label)}</strong>
            <span>${escapeHtml(item.detail)}</span>
        </div>
    `).join('');

    panel.classList.remove('hidden');
    panel.setAttribute('aria-hidden', 'false');
    autocompleteState.visible = true;
    editor.setAttribute('aria-expanded', 'true');
}

function hideAutocompletePanel() {
    const panel = document.getElementById('autocompletePanel');
    const editor = document.getElementById('queryEditor');
    if (panel) {
        panel.classList.add('hidden');
        panel.setAttribute('aria-hidden', 'true');
        panel.innerHTML = '';
    }
    autocompleteState.visible = false;
    autocompleteState.items = [];
    autocompleteState.activeIndex = 0;
    if (editor) {
        editor.setAttribute('aria-expanded', 'false');
    }
}

function acceptAutocompleteSuggestion(index) {
    const editor = document.getElementById('queryEditor');
    if (!editor || !autocompleteState.items.length) {
        return;
    }

    const item = autocompleteState.items[index];
    if (!item) {
        return;
    }

    const value = editor.value;
    const before = value.slice(0, autocompleteState.rangeStart);
    const after = value.slice(autocompleteState.rangeEnd);
    editor.value = `${before}${item.insertText}${after}`;
    const nextCursor = before.length + item.insertText.length;
    editor.selectionStart = editor.selectionEnd = nextCursor;
    hideAutocompletePanel();
    editor.focus();
}

function moveAutocompleteSelection(delta) {
    if (!autocompleteState.visible || !autocompleteState.items.length) {
        return;
    }
    const nextIndex = (autocompleteState.activeIndex + delta + autocompleteState.items.length) % autocompleteState.items.length;
    autocompleteState.activeIndex = nextIndex;
    updateAutocompleteSuggestions();
}

// Handle file upload
async function handleFileUpload(event) {
    const files = event.target.files;
    if (!files || files.length === 0) return;
    
    await handleFiles(files);
    event.target.value = ''; // Reset file input
}

// Handle multiple files
async function handleFiles(files) {
    if (!wasmReady) {
        alert('Please wait for WASM to initialize...');
        return;
    }
    
    // Wait a bit more to ensure WASM functions are available
    let retries = 0;
    while (typeof wasmApi.importFile !== 'function' && retries < 10) {
        console.log('Waiting for WASM functions to be available...');
        await new Promise(resolve => setTimeout(resolve, 100));
        retries++;
    }
    
    if (typeof wasmApi.importFile !== 'function') {
        alert('WASM functions not available. Please refresh the page.');
        return;
    }
    
    for (const file of files) {
        await importSingleFile(file);
    }
}

// Import a single file
async function importSingleFile(file) {
    const fileName = file.name.toLowerCase();
    
    // Check if it's an Excel file
    if (fileName.endsWith('.xlsx') || fileName.endsWith('.xls')) {
        return await importExcelFile(file);
    }
    
    const reader = new FileReader();
    
    reader.onload = async (e) => {
        const content = e.target.result;
        const tableName = sanitizeTableName(file.name);
        
        updateStatus(`Importing ${file.name}...`);
        
        try {
            // Check if WASM functions are available
            if (typeof wasmApi.importFile !== 'function') {
                throw new Error('WASM importFile function not available. Make sure WASM is initialized.');
            }
            
            console.log('Calling WASM importFile with:', file.name, tableName);
            const result = wasmApi.importFile(file.name, content, tableName);
            console.log('WASM importFile result:', result);
            
            if (!result) {
                throw new Error('WASM importFile returned undefined/null');
            }
            
            if (typeof result !== 'object') {
                throw new Error(`WASM importFile returned invalid type: ${typeof result}`);
            }
            
            if (result.success) {
                // Add table to current tables
                const tableInfo = {
                    name: tableName,
                    rowCount: result.rowsImported,
                    columns: Array.isArray(result.columns) ? result.columns.map(c => String(c)) : []
                };
                
                // Update or add table
                const existingIndex = currentTables.findIndex(t => t.name === tableName);
                if (existingIndex >= 0) {
                    currentTables[existingIndex] = tableInfo;
                } else {
                    currentTables.push(tableInfo);
                }
                
                renderTables();
                if (isRoutingGraphFile(file.name)) {
                    loadTables();
                }
                
                let message = `Imported ${result.rowsImported} rows into "${tableName}"`;
                if (result.rowsSkipped > 0) {
                    message += ` (${result.rowsSkipped} skipped)`;
                }
                updateStatus(message);

                // Prefill query editor with a working example for this table
                const editor = document.getElementById('queryEditor');
                const defaultQuery = `SELECT * FROM ${tableName} LIMIT 10`;
                if (!editor.value || /SELECT \* FROM (mytable|table1|table2)/i.test(editor.value)) {
                    editor.value = defaultQuery;
                    syncEditorHighlight();
                    saveEditorState();
                }

                // Ensure Execute is enabled
                const executeBtn = document.getElementById('executeBtn');
                if (executeBtn) executeBtn.disabled = false;
                scheduleDatabaseSnapshotSave();
            } else {
                alert(`Import failed: ${result.error || 'Unknown error'}`);
                updateStatus('Import failed');
                console.error('Import failed:', result);
            }
        } catch (err) {
            alert(`Import error: ${err.message}`);
            updateStatus('Import failed');
        }
    };
    
    reader.onerror = () => {
        alert(`Failed to read file: ${file.name}`);
    };
    
    reader.readAsText(file);
}

// Import Excel file using SheetJS
async function importExcelFile(file) {
    if (typeof XLSX === 'undefined') {
        alert('Excel support library not loaded. Please refresh the page.');
        return;
    }

    updateStatus(`Reading Excel file: ${file.name}...`);

    const reader = new FileReader();
    
    reader.onload = async (e) => {
        try {
            const data = new Uint8Array(e.target.result);
            const workbook = XLSX.read(data, { type: 'array' });
            
            // Import each sheet as a separate table
            for (const sheetName of workbook.SheetNames) {
                const worksheet = workbook.Sheets[sheetName];
                const jsonData = XLSX.utils.sheet_to_json(worksheet);
                
                if (jsonData.length === 0) {
                    console.log(`Sheet "${sheetName}" is empty, skipping`);
                    continue;
                }
                
                const tableName = sanitizeTableName(sheetName);
                const jsonContent = JSON.stringify(jsonData);
                
                updateStatus(`Importing sheet: ${sheetName}...`);
                
                const result = wasmApi.importFile(`${sheetName}.json`, jsonContent, tableName);
                
                if (result && result.success) {
                    const tableInfo = {
                        name: tableName,
                        rowCount: result.rowsImported,
                        columns: Array.isArray(result.columns) ? result.columns.map(c => String(c)) : []
                    };
                    
                    const existingIndex = currentTables.findIndex(t => t.name === tableName);
                    if (existingIndex >= 0) {
                        currentTables[existingIndex] = tableInfo;
                    } else {
                        currentTables.push(tableInfo);
                    }
                }
            }
            
            renderTables();
            updateStatus(`Excel file imported: ${workbook.SheetNames.length} sheet(s)`);
            
            // Enable execute button
            document.getElementById('executeBtn').disabled = false;
            
            // Set example query for first table
            if (currentTables.length > 0) {
                const firstTable = currentTables[0].name;
                document.getElementById('queryEditor').value = `SELECT * FROM ${firstTable} LIMIT 10`;
                syncEditorHighlight();
                saveEditorState();
            }
            scheduleDatabaseSnapshotSave();
        } catch (err) {
            alert(`Failed to parse Excel file: ${err.message}`);
            updateStatus('Excel import failed');
        }
    };
    
    reader.onerror = () => {
        alert(`Failed to read Excel file: ${file.name}`);
    };
    
    reader.readAsArrayBuffer(file);
}

// Sanitize table name
function sanitizeTableName(filename) {
    return filename
        .replace(/\.[^/.]+$/, '') // Remove extension
        .replace(/[^a-zA-Z0-9_]/g, '_') // Replace special chars
        .toLowerCase();
}

function quoteSqlIdentifier(name) {
    const raw = String(name || '').trim();
    if (!raw) {
        return '""';
    }
    if (raw.includes('.')) {
        return raw.split('.').map(part => quoteSqlIdentifier(part)).join('.');
    }
    return `"${raw.replace(/"/g, '""')}"`;
}

// Register a table client-side so it appears in the UI even when WASM is not ready.
function registerClientTable(tableName, rows) {
    const columns = rows.length ? Object.keys(rows[0]).map(c => String(c)) : [];
    const tableInfo = {
        name: tableName,
        rowCount: rows.length,
        columns
    };

    const existingIndex = currentTables.findIndex(t => t.name === tableName);
    if (existingIndex >= 0) {
        currentTables[existingIndex] = tableInfo;
    } else {
        currentTables.push(tableInfo);
    }

    // Save in pending list so it will be imported into WASM later
    pendingClientTables[tableName] = rows;

    renderTables();
    updateStatus(`Registered local table "${tableName}" (${rows.length} rows). Will import into WASM when ready.`);
    // Reveal demo queries when demo tables are registered
    if (Object.prototype.hasOwnProperty.call(DEMO_TABLES, tableName)) {
        showDemoQueries();
    }
}

function showDemoQueries() {
    enhanceDemoQueries();
    updateDemoQueryVisibility();
}

// Render tables in sidebar
function renderTables() {
    const tableList = document.getElementById('tableList');
    const virtuals = window._virtualTables || [];
    const hasAny = currentTables.length > 0 || virtuals.length > 0;

    updateDemoQueryVisibility();
    if (!hasAny) {
        tableList.innerHTML = `
            <div class="empty-state">
                <div class="empty-state-icon">📊</div>
                <div class="empty-state-title">No Tables Loaded</div>
                <div class="empty-state-text">Upload a file to get started</div>
            </div>
        `;
        return;
    }

    let html = '';

    // ── User tables ──────────────────────────────────────────────────────
    if (currentTables.length > 0) {
        html += `<div class="table-section-label">User Tables (${currentTables.length})</div>`;
        html += currentTables.map(table => {
            const isPending = Object.prototype.hasOwnProperty.call(pendingClientTables, table.name);
            const badgeHtml = isPending
                ? `<span class="table-badge pending">pending</span>`
                : `<span class="table-badge imported">imported</span>`;

            return `
            <div class="table-item" onclick="selectTable('${escapeHtml(table.name)}')">
                <div class="table-name">
                    ${escapeHtml(table.name)} ${badgeHtml}
                    <span class="table-remove" onclick="event.stopPropagation(); removeTable('${escapeHtml(table.name)}')" title="Remove table">✕</span>
                    <span class="table-info-btn" onclick="event.stopPropagation(); showTableInfo('${escapeHtml(table.name)}')" title="Show schema">ℹ</span>
                </div>
                <div class="table-meta">
                    <span>📝 ${table.rowCount} rows</span>
                    <span>📁 ${table.columns.length} cols</span>
                </div>
                ${table.columns.length > 0 ? `
                    <div class="table-columns">
                        <div class="table-columns-label">Columns:</div>
                        ${table.columns.map(col => `<span class="column-tag">${escapeHtml(col)}</span>`).join('')}
                    </div>
                ` : ''}
            </div>
        `;
        }).join('');
    }

    // ── Virtual tables ───────────────────────────────────────────────────
    if (virtuals.length > 0) {
        const collapsed = window._virtualCollapsed !== false;
        html += `
            <div class="table-section-label virtual-toggle" onclick="toggleVirtualTables()">
                <span>${collapsed ? '▶' : '▼'} Virtual Tables (${virtuals.length})</span>
            </div>`;
        if (!collapsed) {
            html += virtuals.map(vt => `
                <div class="table-item virtual-table-item" onclick="selectTable('${escapeHtml(vt.name)}')">
                    <div class="table-name">
                        ${escapeHtml(vt.name)}
                        <span class="table-badge virtual">virtual</span>
                        <span class="table-info-btn" onclick="event.stopPropagation(); showTableInfo('${escapeHtml(vt.name)}')" title="Show schema">ℹ</span>
                    </div>
                    <div class="table-meta"><span>computed at query time</span></div>
                </div>
            `).join('');
        }
    }

    tableList.innerHTML = html;
}

// Toggle virtual table section collapsed state
function toggleVirtualTables() {
    window._virtualCollapsed = !(window._virtualCollapsed !== false);
    renderTables();
}

// Show schema / info panel for a table (real or virtual)
function showTableInfo(tableName) {
    if (typeof wasmApi.getTableSchema !== 'function') {
        alert('Schema inspection requires WASM to be ready');
        return;
    }
    const info = wasmApi.getTableSchema(tableName);
    if (!info || !info.success) {
        alert(info?.error || 'Could not load schema');
        return;
    }

    const cols = Array.isArray(info.columns) ? info.columns : [];
    const isVirt = info.virtual === true;
    const rowInfo = isVirt ? 'dynamic' : String(info.rows);

    const panel = document.getElementById('schemaPanel');
    if (panel) {
        panel.setAttribute('aria-hidden', 'false');
        panel.innerHTML = `
            <div class="schema-panel-header">
                <strong id="schemaPanelTitle">${escapeHtml(tableName)}</strong>
                ${isVirt ? '<span class="table-badge virtual">virtual</span>' : ''}
                <button onclick="closeSchemaPanel()" class="schema-close" aria-label="Close schema details">✕</button>
            </div>
            <div class="schema-meta">${rowInfo} rows · ${cols.length} columns</div>
            <table class="schema-table">
                <thead><tr><th>Column</th><th>Type</th></tr></thead>
                <tbody>
                    ${cols.map(c => `<tr><td>${escapeHtml(c.name)}</td><td class="schema-type">${escapeHtml(c.type)}</td></tr>`).join('')}
                </tbody>
            </table>
            <div class="schema-actions">
                <button onclick="setQuery('SELECT * FROM ${quoteSqlIdentifier(tableName)} LIMIT 100'); closeSchemaPanel();">SELECT *</button>
            </div>
        `;
        panel.classList.remove('hidden');
        panel.focus();
    }
}

function closeSchemaPanel() {
    const panel = document.getElementById('schemaPanel');
    if (!panel) {
        return;
    }
    panel.classList.add('hidden');
    panel.setAttribute('aria-hidden', 'true');
}

// Remove table
function removeTable(tableName) {
    const isPending = Object.prototype.hasOwnProperty.call(pendingClientTables, tableName);

    if (!isPending && wasmReady && typeof wasmApi.dropTable === 'function') {
        const result = wasmApi.dropTable(tableName);
        if (!result || !result.success) {
            alert(`Failed to drop table "${tableName}": ${result?.error || 'Unknown error'}`);
            return;
        }
    }

    delete pendingClientTables[tableName];
    currentTables = currentTables.filter(t => t.name !== tableName);
    if (currentResults && currentResults.sourceTable === tableName) {
        currentResults = null;
    }
    renderTables();
    scheduleDatabaseSnapshotSave();
    updateStatus(`Removed table "${tableName}"`);
}

// Select a table
function selectTable(tableName) {
    const query = buildSelectWithColumns(tableName, 10);
    setQuery(query);
}

// Get columns for a table from currentTables or pendingClientTables
function getTableColumns(tableName) {
    const t = currentTables.find(x => x.name === tableName);
    if (t && Array.isArray(t.columns) && t.columns.length) return t.columns.map(c => c);

    const pending = pendingClientTables[tableName];
    if (Array.isArray(pending) && pending.length) {
        return Object.keys(pending[0]);
    }

    return null;
}

// Build a SELECT statement that enumerates all columns instead of using *
function buildSelectWithColumns(tableName, limit) {
    const cols = getTableColumns(tableName);
    const colsPart = Array.isArray(cols) && cols.length
        ? cols.map(c => quoteSqlIdentifier(c)).join(', ')
        : '*';

    const lim = (typeof limit === 'number' && limit > 0) ? ` LIMIT ${limit}` : '';
    return `SELECT ${colsPart} FROM ${quoteSqlIdentifier(tableName)}${lim}`;
}

// Set query in editor
function setQuery(query) {
    const editor = document.getElementById('queryEditor');
    if (!editor) {
        return;
    }
    hideAutocompletePanel();
    editor.value = query;
    syncEditorHighlight();
    saveEditorState();
    closeSidebarOnMobile();
    editor.focus();
}

function closeSidebarOnMobile() {
    if (!window.matchMedia || !window.matchMedia('(max-width: 900px)').matches) {
        return;
    }
    const sidebar = document.querySelector('.sidebar');
    if (sidebar) {
        sidebar.classList.remove('open');
    }
}

// Clear query
function clearQuery() {
    const editor = document.getElementById('queryEditor');
    if (!editor) {
        return;
    }
    hideAutocompletePanel();
    editor.value = '';
    syncEditorHighlight();
    saveEditorState();
    editor.focus();
}

function clearAllTables() {
    if (!confirm('This will remove all imported tables. Continue?')) {
        return;
    }
    if (typeof wasmApi.clearDatabase === 'function') {
        const result = wasmApi.clearDatabase();
        if (!result || !result.success) {
            alert(`Failed to clear database: ${result?.error || 'Unknown error'}`);
            return;
        }
    }
    for (const key of Object.keys(pendingClientTables)) {
        delete pendingClientTables[key];
    }
    currentTables = [];
    currentResults = null;
    resultViewState = {
        filterText: '',
        sortColumn: '',
        sortDirection: 'asc',
    };
    renderTables();
    const resultsContainer = document.getElementById('resultsContainer');
    if (resultsContainer) {
        resultsContainer.innerHTML = `
            <div class="empty-state">
                <div class="empty-state-icon">⚡</div>
                <div class="empty-state-title">Ready to Query</div>
                <div class="empty-state-text">
                    Upload a file and run a SQL query
                </div>
            </div>
        `;
    }
    window.clearVanillaGrid?.();
    storageRemove(DB_SNAPSHOT_KEY);
    renderIntroPage();
    updateStatus('Database cleared');
}

// Format query (basic)
function formatQuery() {
    const editor = document.getElementById('queryEditor');
    if (!editor) {
        return;
    }
    let query = editor.value.trim();
    
    // Basic SQL formatting
    query = query
        .replace(/\s+/g, ' ')
        .replace(/\b(SELECT|FROM|WHERE|JOIN|LEFT JOIN|RIGHT JOIN|INNER JOIN|FULL JOIN|CROSS JOIN|ON|ORDER BY|GROUP BY|HAVING|LIMIT|OFFSET|UNION|UNION ALL|INTERSECT|EXCEPT|WITH)\b/gi, '\n$1')
        .replace(/,/g, ',\n  ')
        .trim();
    
    editor.value = query;
    syncEditorHighlight();
    saveEditorState();
}

// Execute query (UI handler)
async function onExecuteClick() {
    const query = document.getElementById('queryEditor').value.trim();
    
    if (!query) {
        alert('Please enter a query');
        return;
    }

    if (!wasmReady) {
        alert('WASM not ready yet...');
        return;
    }

    const executeBtn = document.getElementById('executeBtn');
    const resultsContainer = document.getElementById('resultsContainer');
    
    executeBtn.disabled = true;
    executeBtn.innerHTML = '<span class="spinner"></span> Running…';
    if (resultsContainer) {
        resultsContainer.setAttribute('aria-busy', 'true');
    }
    setOpenVanillaGridEnabled(false);
    
    updateStatus('Executing query…');

    try {
        if (typeof wasmApi.executeQuery !== 'function') {
            throw new Error('WASM executeQuery function not available');
        }

        const startTime = performance.now();
        // Use executeMulti if available and query contains semicolons
        const hasMulti = query.includes(';') && typeof wasmApi.executeMulti === 'function';
        const result = hasMulti ? wasmApi.executeMulti(query) : wasmApi.executeQuery(query);
        const wallMs = performance.now() - startTime;
        const duration = result?.durationMs != null
            ? result.durationMs.toFixed(2) + ' ms'
            : wallMs.toFixed(1) + ' ms';

        if (result && typeof result === 'object' && result.success) {
            const cols = Array.isArray(result.columns) ? result.columns.map(c => String(c)) : [];
            const rows = Array.isArray(result.rows) ? result.rows : [];
            resultViewState = {
                filterText: '',
                sortColumn: '',
                sortDirection: 'asc',
            };
            currentResults = {
                columns: cols,
                rows: rows,
                rowCount: rows.length,
                duration: duration
            };
            renderResults(currentResults);
            updateStatus(`Query completed: ${currentResults.rowCount} rows in ${duration}${result.statementsRun > 1 ? ` (${result.statementsRun} statements)` : ''}`);
            pushHistory(query, duration, rows.length);
            saveEditorState();
            if (sqlMayMutate(query)) {
                loadTables();
                scheduleDatabaseSnapshotSave();
            }
        } else {
            const errMsg = result && result.error ? result.error : 'Unknown error';
            resultsContainer.innerHTML = `
                <div class="error-message">
                    <strong>Error:</strong> ${escapeHtml(errMsg)}
                </div>
            `;
            window.clearVanillaGrid?.();
            setOpenVanillaGridEnabled(false);
            updateStatus('Query failed');
            pushHistory(query, '0 ms', 'err');
        }
    } catch (error) {
        resultsContainer.innerHTML = `
            <div class="error-message">
                <strong>Error:</strong> ${escapeHtml(error.message)}
            </div>
        `;
        window.clearVanillaGrid?.();
        setOpenVanillaGridEnabled(false);
        updateStatus('Query failed');
    } finally {
        executeBtn.disabled = false;
        executeBtn.innerHTML = '▶ Execute';
        if (resultsContainer) {
            resultsContainer.setAttribute('aria-busy', 'false');
        }
    }
}

// Render query results
function renderResults(data) {
    const resultsContainer = document.getElementById('resultsContainer');
    if (!resultsContainer || !data || !Array.isArray(data.rows) || !Array.isArray(data.columns) || data.rowCount === 0) {
        if (resultsContainer) {
            window.clearVanillaGrid?.();
            setOpenVanillaGridEnabled(false);
            resultsContainer.innerHTML = `
                <div class="empty-state">
                    <div class="empty-state-icon">✓</div>
                    <div class="empty-state-title">No Results</div>
                    <div class="empty-state-text">
                        Query executed successfully but returned no rows
                        <br>Duration: ${data?.duration || '0 ms'}
                    </div>
                </div>
            `;
        }
        return;
    }

    const visible = getVisibleResults(data);
    const displayedRows = visible ? visible.rows : [];
    const displayedColumns = visible ? visible.columns : [];
    const totalRows = data.rows.length;
    const renderedRows = displayedRows.slice(0, RESULT_RENDER_LIMIT);
    const renderIsLimited = displayedRows.length > renderedRows.length;

    if (!visible || displayedRows.length === 0) {
        window.clearVanillaGrid?.();
        setOpenVanillaGridEnabled(false);
        resultsContainer.innerHTML = `
            <div class="empty-state">
                <div class="empty-state-icon">🔎</div>
                <div class="empty-state-title">No Matching Rows</div>
                <div class="empty-state-text">
                    Your current filter removed all rows.
                    <br>Duration: ${data?.duration || '0 ms'}
                </div>
                <div style="margin-top: 12px;">
                    <button onclick="clearResultViewFilters()">Clear Filter</button>
                </div>
            </div>
        `;
        return;
    }

    window.clearVanillaGrid?.();
    const tableHtml = `
        <div class="results-header">
            <div class="results-info">
                <strong>${displayedRows.length}</strong> / <strong>${totalRows}</strong> rows • 
                <strong>${displayedColumns.length}</strong> columns • 
                ${data.duration}
                ${renderIsLimited ? `<br><span>Showing first ${renderedRows.length} rows for browser performance. Export uses the full filtered result.</span>` : ''}
            </div>
            <div class="results-actions">
                <button onclick="copyResultsToClipboard()">Copy Results</button>
                <button id="openVanillaGridBtn" onclick="openInVanillaGrid()" disabled>Open in VanillaGrid</button>
                <button onclick="doExport('csv')">Export CSV</button>
                <button onclick="doExport('tsv')">Export TSV</button>
                <button onclick="doExport('md')">Export Markdown</button>
                <button onclick="doExport('json')">Export JSON</button>
                <button onclick="doExport('xml')">Export XML</button>
            </div>
        </div>
        <div class="results-toolbar">
            <label>
                Filter
                <input id="resultFilterInput" type="search" value="${escapeHtml(resultViewState.filterText)}" placeholder="Search rows..." oninput="updateResultViewState()">
            </label>
            <label>
                Sort by
                <select id="resultSortColumn" onchange="updateResultViewState()">
                    <option value="">None</option>
                    ${data.columns.map((column) => `<option value="${escapeHtml(column)}" ${resultViewState.sortColumn === column ? 'selected' : ''}>${escapeHtml(column)}</option>`).join('')}
                </select>
            </label>
            <label>
                Direction
                <select id="resultSortDirection" onchange="updateResultViewState()">
                    <option value="asc" ${resultViewState.sortDirection === 'asc' ? 'selected' : ''}>Ascending</option>
                    <option value="desc" ${resultViewState.sortDirection === 'desc' ? 'selected' : ''}>Descending</option>
                </select>
            </label>
            <button onclick="clearResultViewFilters()">Reset View</button>
        </div>
        <div class="result-table-wrap">
        <table class="result-table">
            <thead>
                <tr>
                    <th class="row-num-col">#</th>
                    ${displayedColumns.map(col => `
                        <th class="sortable-column" aria-sort="${resultViewState.sortColumn === col ? (resultViewState.sortDirection === 'asc' ? 'ascending' : 'descending') : 'none'}">
                            <button class="column-sort-button" onclick="sortResultsBy(${JSON.stringify(col)})" title="Sort by ${escapeHtml(col)}">
                                <span>${escapeHtml(col)}</span>
                                <span class="column-sort-indicator">${getSortIndicator(col)}</span>
                            </button>
                        </th>
                    `).join('')}
                </tr>
            </thead>
            <tbody>
                ${renderedRows.map((row, idx) => `
                    <tr onclick="this.classList.toggle('selected-row')">
                        <td class="row-num-col">${idx + 1}</td>
                        ${displayedColumns.map(col => {
                            const value = row[col];
                            return formatCell(value);
                        }).join('')}
                    </tr>
                `).join('')}
            </tbody>
        </table>
        </div>
    `;

    resultsContainer.innerHTML = tableHtml;
    setOpenVanillaGridEnabled(true);
}

// Format table cell with truncation for long values
function formatCell(value) {
    if (value === null || value === undefined) {
        return '<td class="null-value">NULL</td>';
    }
    
    if (typeof value === 'number') {
        return `<td class="number-value">${value}</td>`;
    }
    
    if (typeof value === 'boolean') {
        return `<td class="boolean-value">${value}</td>`;
    }
    
    const str = String(value);
    const geo = formatGeoJSONCell(str);
    if (geo) {
        return `<td class="truncated-cell" title="${escapeHtml(str)}">${escapeHtml(geo)}</td>`;
    }
    if (str.length > 120) {
        return `<td class="truncated-cell" title="${escapeHtml(str)}">${escapeHtml(str.slice(0, 120))}…</td>`;
    }
    return `<td>${escapeHtml(str)}</td>`;
}

function formatGeoJSONCell(value) {
    const text = String(value || '').trim();
    if (!text.startsWith('{') || !text.includes('"coordinates"')) {
        return '';
    }
    try {
        const obj = JSON.parse(text);
        if (obj && obj.type === 'Point' && Array.isArray(obj.coordinates)) {
            const lon = Number(obj.coordinates[0]);
            const lat = Number(obj.coordinates[1]);
            if (Number.isFinite(lon) && Number.isFinite(lat)) {
                return `Point(${lon.toFixed(4)}, ${lat.toFixed(4)})`;
            }
        }
        if (obj && typeof obj.type === 'string') {
            return `${obj.type} geometry`;
        }
    } catch (_) {
        return '';
    }
    return '';
}

// Show upload dialog
function showUploadDialog() {
    document.getElementById('fileInput').click();
}

// Load tables (for refresh button)
function loadTables() {
    if (typeof wasmApi.listTables === 'function') {
        try {
            const info = wasmApi.listTables();
            if (info && Array.isArray(info.tables)) {
                // Separate user tables from virtual tables
                const userTbls = info.tables.filter(t => t.kind !== 'virtual');
                const virtTbls = info.tables.filter(t => t.kind === 'virtual');

                // Replace current user-table snapshot from backend state.
                currentTables = userTbls.map(t => ({
                    name: t.name,
                    rowCount: t.rows,
                    columns: Array.isArray(t.columns)
                        ? t.columns.map(c => typeof c === 'object' ? c.name : c)
                        : [],
                    columnTypes: Array.isArray(t.columns)
                        ? t.columns.filter(c => typeof c === 'object')
                        : [],
                    kind: 'table',
                }));

                // Keep pending local tables visible until they are imported.
                for (const [name, rows] of Object.entries(pendingClientTables)) {
                    if (!currentTables.some(t => t.name === name)) {
                        const cols = Array.isArray(rows) && rows.length > 0 ? Object.keys(rows[0]) : [];
                        currentTables.push({
                            name,
                            rowCount: Array.isArray(rows) ? rows.length : 0,
                            columns: cols,
                            kind: 'table',
                        });
                    }
                }

                // Store virtual tables separately
                window._virtualTables = virtTbls.map(t => ({
                    name: t.name,
                    kind: 'virtual',
                    rowCount: -1,
                    columns: [],
                }));

                renderTables();
                updateStatus(`${userTbls.length} table(s), ${virtTbls.length} virtual`);
                return;
            }
        } catch (e) { console.warn('listTables fallback:', e); }
    }
    renderTables();
}

// Toast notification system
let _toastTimeout = null;
function showToast(message, type = 'info') {
    let container = document.getElementById('toastContainer');
    if (!container) {
        container = document.createElement('div');
        container.id = 'toastContainer';
        container.style.cssText = 'position:fixed;bottom:16px;right:16px;z-index:9999;display:flex;flex-direction:column;gap:8px;pointer-events:none;';
        document.body.appendChild(container);
    }
    const toast = document.createElement('div');
    const bg = type === 'error' ? '#5a1d1d' : type === 'success' ? '#1e4d2b' : '#2d2d30';
    const border = type === 'error' ? '#f48771' : type === 'success' ? '#28a745' : '#0e639c';
    toast.style.cssText = `background:${bg};border:1px solid ${border};color:#d4d4d4;padding:10px 16px;border-radius:6px;font-size:13px;box-shadow:0 4px 12px rgba(0,0,0,0.3);pointer-events:auto;opacity:0;transition:opacity 0.3s;max-width:400px;`;
    toast.textContent = message;
    container.appendChild(toast);
    requestAnimationFrame(() => { toast.style.opacity = '1'; });
    setTimeout(() => {
        toast.style.opacity = '0';
        setTimeout(() => toast.remove(), 300);
    }, 3000);
}

// Update status
function updateStatus(text) {
    const statusText = document.getElementById('statusText');
    if (statusText) {
        statusText.textContent = text;
    }
}

function setOpenVanillaGridEnabled(enabled) {
    const btn = document.getElementById('openVanillaGridBtn');
    if (btn) {
        btn.disabled = !enabled;
    }
}

function openInVanillaGrid() {
    const visible = getVisibleResults();
    if (!visible || !Array.isArray(visible.rows) || visible.rows.length === 0) {
        alert('No results to visualize yet. Execute a query with rows first.');
        return;
    }
    window.renderVanillaGrid?.(visible);
}

function getVisibleResults(source = currentResults) {
    if (!source || !Array.isArray(source.rows) || !Array.isArray(source.columns)) {
        return null;
    }

    let rows = source.rows.slice();
    const filterText = resultViewState.filterText.trim().toLowerCase();
    if (filterText) {
        rows = rows.filter((row) => source.columns.some((column) => {
            const value = row[column];
            if (value === null || value === undefined) {
                return false;
            }
            return String(value).toLowerCase().includes(filterText);
        }));
    }

    if (resultViewState.sortColumn && source.columns.includes(resultViewState.sortColumn)) {
        const column = resultViewState.sortColumn;
        const direction = resultViewState.sortDirection === 'desc' ? -1 : 1;
        rows.sort((leftRow, rightRow) => direction * compareResultValues(leftRow[column], rightRow[column]));
    }

    return {
        columns: source.columns.slice(),
        rows,
        rowCount: rows.length,
        duration: source.duration,
    };
}

function compareResultValues(leftValue, rightValue) {
    if (leftValue === rightValue) {
        return 0;
    }
    if (leftValue === null || leftValue === undefined) {
        return 1;
    }
    if (rightValue === null || rightValue === undefined) {
        return -1;
    }

    const leftNumber = typeof leftValue === 'number' ? leftValue : Number(leftValue);
    const rightNumber = typeof rightValue === 'number' ? rightValue : Number(rightValue);
    const leftIsNumeric = Number.isFinite(leftNumber);
    const rightIsNumeric = Number.isFinite(rightNumber);

    if (leftIsNumeric && rightIsNumeric) {
        return leftNumber - rightNumber;
    }

    const leftText = String(leftValue);
    const rightText = String(rightValue);
    return leftText.localeCompare(rightText, undefined, { numeric: true, sensitivity: 'base' });
}

function updateResultViewState() {
    const filterInput = document.getElementById('resultFilterInput');
    const sortSelect = document.getElementById('resultSortColumn');
    const sortDirection = document.getElementById('resultSortDirection');
    const keepFilterFocus = document.activeElement && document.activeElement.id === 'resultFilterInput';
    const selectionStart = keepFilterFocus && typeof filterInput?.selectionStart === 'number' ? filterInput.selectionStart : null;
    const selectionEnd = keepFilterFocus && typeof filterInput?.selectionEnd === 'number' ? filterInput.selectionEnd : null;

    if (filterInput) {
        resultViewState.filterText = filterInput.value;
    }
    if (sortSelect) {
        resultViewState.sortColumn = sortSelect.value;
    }
    if (sortDirection) {
        resultViewState.sortDirection = sortDirection.value;
    }

    if (currentResults) {
        renderResults(currentResults);
        if (keepFilterFocus) {
            const refreshedFilter = document.getElementById('resultFilterInput');
            if (refreshedFilter) {
                refreshedFilter.focus();
                if (selectionStart !== null && selectionEnd !== null && typeof refreshedFilter.setSelectionRange === 'function') {
                    refreshedFilter.setSelectionRange(selectionStart, selectionEnd);
                }
            }
        }
    }
}

function clearResultViewFilters() {
    resultViewState.filterText = '';
    resultViewState.sortColumn = '';
    resultViewState.sortDirection = 'asc';
    if (currentResults) {
        renderResults(currentResults);
    }
}

function sortResultsBy(column) {
    if (!currentResults || !Array.isArray(currentResults.columns) || !currentResults.columns.includes(column)) {
        return;
    }

    if (resultViewState.sortColumn === column) {
        resultViewState.sortDirection = resultViewState.sortDirection === 'asc' ? 'desc' : 'asc';
    } else {
        resultViewState.sortColumn = column;
        resultViewState.sortDirection = 'asc';
    }

    renderResults(currentResults);
}

function getSortIndicator(column) {
    if (resultViewState.sortColumn !== column) {
        return '';
    }
    return resultViewState.sortDirection === 'asc' ? '▲' : '▼';
}

// Unified export dispatcher – tries WASM-side first, falls back to client-side
function doExport(format) {
    const visible = getVisibleResults();
    if (!visible || !visible.rows || visible.rows.length === 0) {
        alert('No results to export');
        return;
    }
    const viewIsRaw = !resultViewState.filterText.trim() && !resultViewState.sortColumn;
    // Try WASM-side exporter
    if (viewIsRaw && typeof wasmApi.exportResults === 'function') {
        try {
            const res = wasmApi.exportResults(format);
            if (res && res.success && res.data) {
                const mimeType = (typeof res.mimeType === 'string' && res.mimeType) ||
                    (typeof res.mime === 'string' && res.mime) ||
                    'application/octet-stream';
                const ext = (typeof res.ext === 'string' && res.ext) ? res.ext : format;
                downloadFile(res.data, `query_results.${ext}`, mimeType);
                return;
            }
        } catch (_) { /* fall through */ }
    }
    // Client-side fallback
    if (format === 'csv') exportCSV(visible);
    else if (format === 'tsv') exportTSV(visible);
    else if (format === 'md') exportMarkdown(visible);
    else if (format === 'json') exportJSON(visible);
    else if (format === 'xml') exportXML(visible);
    else alert('Unsupported export format: ' + format);
}

// Export to CSV
function exportCSV(visible = getVisibleResults()) {
    if (!visible || !visible.rows || visible.rows.length === 0) {
        alert('No results to export');
        return;
    }
    
    let csv = visible.columns.join(',') + '\n';
    
    visible.rows.forEach(row => {
        const values = visible.columns.map(col => {
            let value = row[col];
            if (value === null || value === undefined) {
                return '';
            }
            value = String(value);
            if (value.includes(',') || value.includes('"') || value.includes('\n')) {
                value = '"' + value.replace(/"/g, '""') + '"';
            }
            return value;
        });
        csv += values.join(',') + '\n';
    });
    
    downloadFile(csv, 'query_results.csv', 'text/csv');
}

function exportTSV(visible = getVisibleResults()) {
    if (!visible || !visible.rows || visible.rows.length === 0) {
        alert('No results to export');
        return;
    }

    const escapeTsv = (value) => {
        if (value === null || value === undefined) {
            return '';
        }
        return String(value).replace(/\t/g, ' ').replace(/\r?\n/g, ' ');
    };

    const lines = [visible.columns.join('\t')];
    visible.rows.forEach((row) => {
        lines.push(visible.columns.map((column) => escapeTsv(row[column])).join('\t'));
    });

    downloadFile(lines.join('\n'), 'query_results.tsv', 'text/tab-separated-values');
}

function exportMarkdown(visible = getVisibleResults()) {
    if (!visible || !visible.rows || visible.rows.length === 0) {
        alert('No results to export');
        return;
    }

    const escapeMd = (value) => String(value ?? '').replace(/\|/g, '\\|').replace(/\n/g, ' ');
    const header = `| ${visible.columns.map(escapeMd).join(' | ')} |`;
    const separator = `| ${visible.columns.map(() => '---').join(' | ')} |`;
    const body = visible.rows.map((row) => `| ${visible.columns.map((column) => escapeMd(row[column])).join(' | ')} |`);
    downloadFile([header, separator, ...body].join('\n'), 'query_results.md', 'text/markdown');
}

// Export to JSON
function exportJSON(visible = getVisibleResults()) {
    if (!visible || !visible.rows || visible.rows.length === 0) {
        alert('No results to export');
        return;
    }
    
    const json = JSON.stringify(visible.rows, null, 2);
    downloadFile(json, 'query_results.json', 'application/json');
}

// Export to XML (client-side fallback)
function exportXML(visible = getVisibleResults()) {
    if (!visible || !visible.rows || visible.rows.length === 0) {
        alert('No results to export');
        return;
    }
    let xml = '<?xml version="1.0" encoding="UTF-8"?>\n<results>\n';
    visible.rows.forEach(row => {
        xml += '  <row>\n';
        visible.columns.forEach(col => {
            const val = row[col];
            const tag = toXmlTag(col);
            xml += `    <${tag}>${escapeXml(val == null ? '' : String(val))}</${tag}>\n`;
        });
        xml += '  </row>\n';
    });
    xml += '</results>\n';
    downloadFile(xml, 'query_results.xml', 'application/xml');
}

// Download file helper
function downloadFile(content, filename, mimeType) {
    const blob = new Blob([content], { type: mimeType });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = filename;
    a.style.display = 'none';
    document.body.appendChild(a);
    a.click();
    a.remove();
    setTimeout(() => URL.revokeObjectURL(url), 0);
}

async function copyTextToClipboard(text) {
    if (navigator.clipboard && typeof navigator.clipboard.writeText === 'function') {
        await navigator.clipboard.writeText(text);
        return true;
    }

    const fallback = document.createElement('textarea');
    fallback.value = text;
    fallback.setAttribute('readonly', 'true');
    fallback.style.position = 'absolute';
    fallback.style.left = '-9999px';
    document.body.appendChild(fallback);
    fallback.select();
    const ok = document.execCommand('copy');
    fallback.remove();
    return ok;
}

function copyQueryToClipboard() {
    const editor = document.getElementById('queryEditor');
    if (!editor || !editor.value.trim()) {
        alert('No SQL query to copy');
        return;
    }

    copyTextToClipboard(editor.value)
        .then(() => {
            updateStatus('SQL query copied to clipboard');
            showToast('SQL query copied to clipboard', 'success');
        })
        .catch((error) => {
            showToast(`Copy failed: ${error.message}`, 'error');
        });
}

function copyResultsToClipboard() {
    const visible = getVisibleResults();
    if (!visible || !Array.isArray(visible.rows) || visible.rows.length === 0) {
        alert('No query results to copy');
        return;
    }

    const header = visible.columns.join('\t');
    const rows = visible.rows.map((row) =>
        visible.columns.map((column) => {
            const value = row[column];
            return value === null || value === undefined ? '' : String(value).replace(/\t/g, ' ').replace(/\r?\n/g, ' ');
        }).join('\t')
    );

    copyTextToClipboard([header, ...rows].join('\n'))
        .then(() => {
            updateStatus('Results copied to clipboard');
            showToast('Results copied to clipboard', 'success');
        })
        .catch((error) => {
            showToast(`Copy failed: ${error.message}`, 'error');
        });
}

function detectClipboardImportFormat(text) {
    const trimmed = String(text || '').trim();
    if (!trimmed) {
        return 'csv';
    }
    if (trimmed.startsWith('{') || trimmed.startsWith('[')) {
        return 'json';
    }

    const lines = trimmed.split(/\r?\n/).filter(Boolean);
    if (lines.length > 1 && lines.every((line) => line.trim().startsWith('{') && line.trim().endsWith('}'))) {
        return 'jsonl';
    }

    const sample = lines.slice(0, 5).join('\n');
    const tabCount = (sample.match(/\t/g) || []).length;
    const commaCount = (sample.match(/,/g) || []).length;
    return tabCount > commaCount ? 'tsv' : 'csv';
}

async function importClipboardData() {
    let text = '';
    try {
        if (navigator.clipboard && typeof navigator.clipboard.readText === 'function') {
            text = await navigator.clipboard.readText();
        }
    } catch (_) {
        // Fall back to prompt below.
    }

    if (!text || !text.trim()) {
        text = prompt('Paste CSV, TSV, JSON, or JSONL data to import:');
        if (text === null) {
            return;
        }
    }

    const format = detectClipboardImportFormat(text);
    const defaultTableName = `clipboard_${format}`;
    const tableNameInput = prompt('Table name for imported clipboard data:', defaultTableName);
    if (tableNameInput === null) {
        return;
    }

    const tableName = sanitizeTableName(tableNameInput) || defaultTableName;
    const ext = format === 'json' ? '.json' : format === 'jsonl' ? '.jsonl' : format === 'tsv' ? '.tsv' : '.csv';

    if (!wasmReady || typeof wasmApi.importFile !== 'function') {
        alert('WASM import is not ready yet');
        return;
    }

    updateStatus(`Importing clipboard data into ${tableName}...`);
    const result = wasmApi.importFile(`${tableName}${ext}`, text, tableName);
    if (result && result.success) {
        loadTables();
        const editor = document.getElementById('queryEditor');
        if (editor && !editor.value.trim()) {
            editor.value = `SELECT * FROM ${quoteSqlIdentifier(tableName)} LIMIT 10`;
            syncEditorHighlight();
            saveEditorState();
        }
        scheduleDatabaseSnapshotSave();
        updateStatus(`Imported clipboard data into "${tableName}" (${result.rowsImported} rows)`);
        return;
    }

    alert(`Clipboard import failed: ${result?.error || 'Unknown error'}`);
    updateStatus('Clipboard import failed');
}

// Escape HTML – static map avoids creating DOM nodes on every call
function escapeHtml(text) {
    return String(text)
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#39;');
}

// Escape XML special chars
function escapeXml(s) {
    return s.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;').replace(/'/g,'&apos;');
}

function toXmlTag(name) {
    const raw = String(name || '').trim();
    if (!raw) return 'col';
    let tag = raw.replace(/[^A-Za-z0-9_.-]/g, '_');
    if (!/^[A-Za-z_]/.test(tag)) {
        tag = `c_${tag}`;
    }
    return tag || 'col';
}

// ----- Query History -----
function pushHistory(sql, duration, rows) {
    queryHistory.unshift({ sql, duration, rows, ts: Date.now() });
    if (queryHistory.length > MAX_HISTORY) queryHistory.length = MAX_HISTORY;
    try { localStorage.setItem(HISTORY_KEY, JSON.stringify(queryHistory)); } catch (_) {}
    renderHistory();
}

function clearHistory() {
    queryHistory.length = 0;
    try {
        localStorage.removeItem(HISTORY_KEY);
        localStorage.removeItem('tinySQL_history');
        localStorage.removeItem('tsql_history');
    } catch (_) {}
    renderHistory();
}

function renderHistory() {
    const panel = document.getElementById('historyList');
    if (!panel) return;
    if (queryHistory.length === 0) {
        panel.innerHTML = '<div class="empty-state-text">No queries yet</div>';
        return;
    }
    panel.innerHTML = queryHistory.map((h, i) =>
        `<div class="history-item" onclick="recallHistory(${i})" title="${escapeHtml(h.sql)}">
            <div class="history-sql">${escapeHtml(h.sql.length > 80 ? h.sql.slice(0,77)+'…' : h.sql)}</div>
            <div class="history-meta">${h.rows} rows · ${h.duration} · ${timeAgo(h.ts)}</div>
        </div>`
    ).join('');
}

function recallHistory(idx) {
    const h = queryHistory[idx];
    if (h) {
        setQuery(h.sql);
    }
}

function timeAgo(ts) {
    const sec = Math.floor((Date.now() - ts) / 1000);
    if (sec < 60) return 'just now';
    if (sec < 3600) return Math.floor(sec/60) + 'm ago';
    if (sec < 86400) return Math.floor(sec/3600) + 'h ago';
    return Math.floor(sec/86400) + 'd ago';
}

function toggleHistory() {
    const panel = document.getElementById('historyPanel');
    if (panel) panel.classList.toggle('hidden');
}

// Keyboard shortcuts
document.addEventListener('DOMContentLoaded', () => {
    const editor = document.getElementById('queryEditor');
    if (!editor) {
        return;
    }

    // Sidebar toggle for mobile
    window.toggleSidebar = function () {
        const sidebar = document.querySelector('.sidebar');
        if (sidebar) sidebar.classList.toggle('open');
    };
    // Close sidebar on click outside (mobile)
    document.addEventListener('click', (e) => {
        const sidebar = document.querySelector('.sidebar');
        const toggle = document.querySelector('.sidebar-toggle');
        if (sidebar && sidebar.classList.contains('open') && !sidebar.contains(e.target) && e.target !== toggle) {
            sidebar.classList.remove('open');
        }
    });

    editor.addEventListener('keydown', (event) => {
        if (autocompleteState.visible && autocompleteState.items.length > 0) {
            if (event.key === 'ArrowDown' || event.key === 'Down') {
                event.preventDefault();
                moveAutocompleteSelection(1);
                return;
            }
            if (event.key === 'ArrowUp' || event.key === 'Up') {
                event.preventDefault();
                moveAutocompleteSelection(-1);
                return;
            }
            if (event.key === 'Tab' || event.key === 'Enter' || event.key === 'Return') {
                event.preventDefault();
                acceptAutocompleteSuggestion(autocompleteState.activeIndex);
                return;
            }
            if (event.key === 'Escape' || event.key === 'Esc') {
                event.preventDefault();
                hideAutocompletePanel();
                return;
            }
        }

        // Ctrl/Cmd + Enter to execute
        if ((event.ctrlKey || event.metaKey) && event.key === 'Enter') {
            event.preventDefault();
            onExecuteClick();
            return;
        }

        // Ctrl/Cmd + Space opens general suggestions
        if ((event.ctrlKey || event.metaKey) && (event.key === ' ' || event.code === 'Space' || event.key.toLowerCase() === 'space')) {
            event.preventDefault();
            updateAutocompleteSuggestions(true);
            return;
        }

        // Tab / Shift+Tab for indentation
        if (event.key === 'Tab') {
            event.preventDefault();
            const start = event.target.selectionStart;
            const end = event.target.selectionEnd;
            if (event.shiftKey) {
                // Unindent: remove up to 2 leading spaces on each selected line
                const before = event.target.value.substring(0, start);
                const selected = event.target.value.substring(start, end);
                const after = event.target.value.substring(end);
                const lineStart = before.lastIndexOf('\n') + 1;
                const block = event.target.value.substring(lineStart, end);
                const unindented = block.replace(/^( {1,2})/gm, '');
                const diff = block.length - unindented.length;
                event.target.value = event.target.value.substring(0, lineStart) + unindented + after;
                event.target.selectionStart = Math.max(lineStart, start - Math.min(2, before.length - lineStart));
                event.target.selectionEnd = end - diff;
            } else {
                event.target.value = event.target.value.substring(0, start) + '  ' + event.target.value.substring(end);
                event.target.selectionStart = event.target.selectionEnd = start + 2;
            }
            syncEditorHighlight();
            return;
        }

        // Auto-close brackets and quotes
        const AUTO_PAIRS = { '(': ')', "'": "'", '"': '"' };
        if (AUTO_PAIRS[event.key] && !event.ctrlKey && !event.metaKey) {
            const start = event.target.selectionStart;
            const end = event.target.selectionEnd;
            const selected = event.target.value.substring(start, end);
            if (selected.length > 0) {
                // Wrap selection
                event.preventDefault();
                const wrapped = event.key + selected + AUTO_PAIRS[event.key];
                event.target.value = event.target.value.substring(0, start) + wrapped + event.target.value.substring(end);
                event.target.selectionStart = start + 1;
                event.target.selectionEnd = end + 1;
                syncEditorHighlight();
                return;
            }
        }

        // ArrowUp in empty editor recalls last query
        if (event.key === 'ArrowUp' && editor.value.trim() === '' && queryHistory.length > 0) {
            event.preventDefault();
            editor.value = queryHistory[0].sql;
            syncEditorHighlight();
            return;
        }

        // Ctrl/Cmd + Shift + F to format SQL
        if ((event.ctrlKey || event.metaKey) && event.shiftKey && event.key === 'F') {
            event.preventDefault();
            if (typeof formatQuery === 'function') {
                formatQuery();
            }
        }
    });
});
