// TinySQL Query Files - WASM Application
let wasmReady = false;
let currentTables = [];
let currentResults = null;
const HISTORY_KEY = 'tinysql_query_history_v1';
// Kept only to migrate the old synchronous localStorage snapshot once.
const DB_SNAPSHOT_KEY = 'tinysql_query_files_db_snapshot_v1';
const EDITOR_STATE_KEY = 'tinysql_query_files_editor_v1';
const ACTIVE_WORKSPACE_KEY = 'tinysql_query_files_active_workspace_v1';
const DEFAULT_WORKSPACE_ID = 'default';
const DEFAULT_WORKSPACE_NAME = 'My workspace';
const DEFAULT_RESULT_PAGE_SIZE = 100;
const MAX_IMPORT_BYTES = 64 * 1024 * 1024;
const RESULT_PAGE_SIZES = [50, 100, 250, 500];
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
    'ST_SIMPLIFY', 'ST_BBOX', 'ST_CENTROID', 'ST_AFFINE', 'ST_SMOOTH', 'ST_REMOVE_HOLES', 'ST_CLEAN', 'ST_SNAPTOGRID', 'ST_ISVALID',
    'ST_AZIMUTH', 'ST_PROJECT', 'ST_MIDPOINT', 'ST_WITHIN', 'ST_CONTAINS', 'ST_COVERS', 'ST_COVEREDBY',
    'ST_TOUCHES', 'ST_AREA', 'ST_LENGTH', 'ST_PERIMETER', 'ST_GEOMFROMTEXT', 'ST_GEOMFROMEWKT',
    'ST_GEOMFROMWKB', 'ST_ASTEXT', 'ST_ASEWKT', 'ST_ASBINARY', 'ST_ASGEOJSON', 'ST_TRANSFORM',
    'GEO_POINT', 'GEO_DISTANCE', 'GEO_WITHIN_BBOX', 'GEO_SIMPLIFY', 'GEO_BBOX', 'GEO_CENTROID',
    'GEO_AFFINE', 'GEO_SMOOTH', 'GEO_DROP_HOLES', 'GEO_CLEAN', 'GEO_SNAP', 'GEO_IS_VALID', 'GEO_BEARING', 'GEO_DESTINATION', 'GEO_MIDPOINT',
    'GEO_WITHIN_POLYGON', 'GEO_POLYGON_AREA', 'GEO_LENGTH', 'FTS_MATCH', 'FTS_RANK', 'FTS_SEARCH',
    'FTS_SNIPPET', 'BM25', 'CONTAINS_ALL', 'CONTAINS_ANY', 'CONTAINS_SCORE',
    'VEC_FROM_JSON', 'VEC_SEARCH', 'VEC_COSINE_SIMILARITY', 'VEC_BINARY_QUANTIZE',
    'VEC_HAMMING_DISTANCE', 'VEC_CENTROID', 'VEC_DISTANCE', 'HYBRID_SEARCH', 'VEC_HYBRID_SEARCH',
    'RAG_CONTEXT', 'RAG_CONTEXT_FROM', 'RAG_SEARCH', 'RAG_WARM', 'RAG_HYBRID_SCORE', 'RAG_RANK_SCORE',
    'VEC_SEARCH_FILTERED', 'FTS_SEARCH_FILTERED', 'ROUTE_SHORTEST_PATH', 'ROUTE_DISTANCE', 'ROUTE_WARM',
    'GEO_GEOHASH_ENCODE', 'GEO_GEOHASH_DECODE', 'GEO_GEOHASH_BBOX', 'GEO_GEOHASH_NEIGHBORS',
    'GPKG_SRID', 'GPKG_HEADER', 'GPKG_BBOX', 'GPKG_AS_WKB', 'GEO_FROM_GPKG',
    'CRS_NORMALIZE', 'CRS_URI', 'CRS_AXIS_ORDER', 'CRS_INFO', 'WMS_BBOX',
    'TILE_MATRIX_BBOX', 'TILE_MATRIX_POSITION', 'RECENCY_SCORE', 'HASH', 'URL_PARSE', 'YAML_GET',
    'CALL', 'ANALYZE', 'ROUND'
];
// Safe references to WASM-exported functions (set after init)
let wasmApi = {
    importFile: null,
    executeQuery: null,
    executeQueryStream: null,
    executeMulti: null,
    getResultPage: null,
    clearDatabase: null,
    dropTable: null,
    listTables: null,
    exportResults: null,
    getTableSchema: null,
    exportDatabase: null,
    exportDatabaseBytes: null,
    importDatabase: null,
    importDatabaseBytes: null,
    validateDatabaseBytes: null,
    getRuntimeStatus: null,
    setRuntimeIdentity: null,
};
let wasmEngine = null;
let workspaceStore = null;
let activeWorkspaceId = DEFAULT_WORKSPACE_ID;
let activeWorkspaceName = DEFAULT_WORKSPACE_NAME;
let legacySnapshotMigrationPending = false;
let legacySnapshotMigrationWorkspaceId = null;
let workspaceTransition = null;
let workspaceEpoch = 0;
let queryExecutionInFlight = false;
let activeQueryAbortController = null;
let activeQueryStreamProgress = null;
let streamPreviewRenderTimer = null;
const activeWorkerRequests = new Map();
const workspaceChangingMethods = new Set([
    'importFile', 'executeQuery', 'executeQueryStream', 'executeMulti', 'clearDatabase', 'dropTable',
    'importDatabase', 'importDatabaseBytes',
]);

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
    page: 1,
    pageSize: DEFAULT_RESULT_PAGE_SIZE,
};
let editorSaveTimer = null;
let snapshotSaveTimer = null;
let snapshotIdleHandle = null;
let snapshotDirty = false;
let snapshotRevision = 0;
let applyingHashDemo = false;
let lastAppliedHash = '';
let runtimeRefreshTimer = null;
let runtimePanelOpen = false;
let runtimeStatusRequest = null;

function getRuntimeSessionId() {
    const key = 'tinysql_runtime_session_v1';
    try {
        let value = sessionStorage.getItem(key);
        if (!value) {
            value = globalThis.crypto?.randomUUID?.() || `session-${Date.now()}-${Math.random().toString(16).slice(2)}`;
            sessionStorage.setItem(key, value);
        }
        return value;
    } catch (_) {
        return `session-${Date.now()}`;
    }
}

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

function base64ToSnapshotBytes(encoded) {
    const binary = atob(String(encoded || ''));
    const bytes = new Uint8Array(binary.length);
    for (let index = 0; index < binary.length; index += 1) {
        bytes[index] = binary.charCodeAt(index);
    }
    return bytes.buffer;
}

async function snapshotBytesToBase64(snapshot) {
    if (typeof snapshot === 'string') return snapshot;
    let buffer = snapshot;
    if (typeof Blob !== 'undefined' && snapshot instanceof Blob) {
        buffer = await snapshot.arrayBuffer();
    }
    const bytes = buffer instanceof ArrayBuffer
        ? new Uint8Array(buffer)
        : ArrayBuffer.isView(buffer)
            ? new Uint8Array(buffer.buffer, buffer.byteOffset, buffer.byteLength)
            : null;
    if (!bytes) throw new Error('Workspace snapshot has an unsupported binary format.');

    // Avoid spreading a large import into one call stack frame.
    const parts = [];
    for (let offset = 0; offset < bytes.length; offset += 0x8000) {
        parts.push(String.fromCharCode(...bytes.subarray(offset, offset + 0x8000)));
    }
    return btoa(parts.join(''));
}

function isBinarySnapshot(snapshot) {
    return (typeof ArrayBuffer !== 'undefined' && snapshot instanceof ArrayBuffer)
        || (typeof ArrayBuffer !== 'undefined' && ArrayBuffer.isView(snapshot))
        || (typeof Blob !== 'undefined' && snapshot instanceof Blob);
}

async function exportWorkspaceSnapshot() {
    if (typeof wasmApi.exportDatabaseBytes === 'function') {
        const result = await wasmApi.exportDatabaseBytes();
        if (!result?.success) {
            throw new Error(result?.error || 'Could not export the binary workspace snapshot.');
        }
        if (!isBinarySnapshot(result.data)) {
            throw new Error('The local engine returned an invalid binary workspace snapshot.');
        }
        return { result, snapshot: result.data, format: result.format || 'tinysql-gob' };
    }

    // Old cached WASM bundles do not expose the binary API. Preserve their
    // workspaces by decoding the legacy Base64 response before IndexedDB sees
    // it; a subsequent modern build will restore it through the raw API.
    const result = await wasmApi.exportDatabase();
    if (!result?.success || typeof result.data !== 'string') {
        throw new Error(result?.error || 'Could not export the workspace snapshot.');
    }
    return { result, snapshot: base64ToSnapshotBytes(result.data), format: 'tinysql-gob' };
}

async function importWorkspaceSnapshot(snapshot) {
    if (typeof wasmApi.importDatabaseBytes === 'function' && isBinarySnapshot(snapshot)) {
        return wasmApi.importDatabaseBytes(snapshot);
    }
    if (typeof wasmApi.importDatabase !== 'function') {
        throw new Error('The local engine does not support workspace restore.');
    }
    return wasmApi.importDatabase(await snapshotBytesToBase64(snapshot));
}

async function validateWorkspaceSnapshot(snapshot) {
    if (typeof wasmApi.validateDatabaseBytes === 'function' && isBinarySnapshot(snapshot)) {
        return wasmApi.validateDatabaseBytes(snapshot);
    }
    // Compatibility fallback for an old cached WASM bundle. Modern builds
    // never mutate the active database while probing a recovery candidate.
    // Keep the candidate bytes intact: the selected generation must be
    // imported once more after recovery has chosen it.
    if (typeof wasmApi.importDatabase === 'function') {
        return wasmApi.importDatabase(await snapshotBytesToBase64(snapshot));
    }
    return importWorkspaceSnapshot(snapshot);
}

function getLegacySnapshot() {
    const raw = storageGet(DB_SNAPSHOT_KEY);
    if (!raw) return null;
    try {
        const payload = JSON.parse(raw);
        if (payload && typeof payload.data === 'string') return payload.data;
    } catch (_) {
        // Legacy releases wrote the raw base64 snapshot without an envelope.
    }
    return raw;
}

function completeLegacySnapshotMigration(workspaceId, restored) {
    if (!legacySnapshotMigrationPending || workspaceId !== legacySnapshotMigrationWorkspaceId || !restored?.snapshot) {
        return;
    }
    storageRemove(DB_SNAPSHOT_KEY);
    legacySnapshotMigrationPending = false;
    legacySnapshotMigrationWorkspaceId = null;
}

function isWorkspaceTransitioning() {
    return workspaceTransition !== null;
}

function hasActiveWorkspaceDataOperation() {
    return [...activeWorkerRequests.values()].some((method) => workspaceChangingMethods.has(method));
}

function setWorkspaceControlsBusy(busy) {
    document.body.classList.toggle('workspace-transitioning', busy);
    const selector = document.getElementById('workspaceSelect');
    if (selector) selector.disabled = busy || !workspaceStore;
    const panel = document.getElementById('workspacePanel');
    panel?.setAttribute('aria-busy', busy ? 'true' : 'false');
    for (const control of document.querySelectorAll(
        '#createWorkspaceButton, #workspaceCreateButton, #workspaceNameInput, [data-workspace-action="open"]'
    )) {
        control.disabled = busy;
    }
}

function runWorkspaceTransition(task) {
    if (workspaceTransition) {
        showToast('A workspace change is already in progress.', 'info');
        return Promise.resolve(false);
    }
    if (queryExecutionInFlight) {
        showToast('Wait for the current query to finish before changing workspaces.', 'info');
        return Promise.resolve(false);
    }
    if (hasActiveWorkspaceDataOperation()) {
        showToast('Wait for the current data operation to finish before changing workspaces.', 'info');
        return Promise.resolve(false);
    }
    const transition = Promise.resolve().then(async () => {
        setWorkspaceControlsBusy(true);
        try {
            return await task();
        } finally {
            setWorkspaceControlsBusy(false);
        }
    });
    workspaceTransition = transition;
    return transition.finally(() => {
        if (workspaceTransition === transition) workspaceTransition = null;
    });
}

function requireStableWorkspace(action = 'continue') {
    if (!isWorkspaceTransitioning()) return true;
    showToast(`Please wait for the workspace change to finish before you ${action}.`, 'info');
    return false;
}

function resetWorkspaceResultView({ showIntro = false } = {}) {
    currentResults = null;
    resultViewState = {
        filterText: '',
        sortColumn: '',
        sortDirection: 'asc',
        page: 1,
        pageSize: DEFAULT_RESULT_PAGE_SIZE,
    };
    const resultsContainer = document.getElementById('resultsContainer');
    if (resultsContainer) {
        resultsContainer.setAttribute('aria-busy', 'false');
        resultsContainer.innerHTML = `
            <div class="empty-state">
                <div class="empty-state-icon">⚡</div>
                <div class="empty-state-title">Ready to Query</div>
                <div class="empty-state-text">Upload a file and run a SQL query</div>
            </div>
        `;
    }
    document.getElementById('schemaPanel')?.classList.add('hidden');
    window.clearVanillaGrid?.();
    setOpenVanillaGridEnabled(false);
    if (showIntro) renderIntroPage();
}

async function initializeWorkspaceStorage() {
    if (workspaceStore) return true;
    const Storage = globalThis.TinySQLWorkspaceStorage?.WorkspaceStorage;
    if (typeof Storage !== 'function') {
        console.warn('Workspace storage module is unavailable.');
        return false;
    }
    try {
        workspaceStore = new Storage({ maxGenerations: 3, preferOPFS: true });
        await workspaceStore.open();
        let defaultWorkspace = null;
        try {
            defaultWorkspace = await workspaceStore.getWorkspace(DEFAULT_WORKSPACE_ID);
        } catch (error) {
            if (error?.code !== 'WORKSPACE_NOT_FOUND') throw error;
        }
        if (!defaultWorkspace) {
            const legacySnapshot = getLegacySnapshot();
            if (legacySnapshot) {
                try {
                    defaultWorkspace = await workspaceStore.importLegacySnapshot(base64ToSnapshotBytes(legacySnapshot), {
                        id: DEFAULT_WORKSPACE_ID,
                        name: DEFAULT_WORKSPACE_NAME,
                        format: 'tinysql-gob',
                        metadata: { migratedAt: new Date().toISOString() },
                    });
                    legacySnapshotMigrationPending = true;
                    legacySnapshotMigrationWorkspaceId = DEFAULT_WORKSPACE_ID;
                } catch (error) {
                    // Keep the legacy value untouched: a future release or a
                    // manual recovery may still be able to read it. A broken
                    // legacy value must not disable every new workspace.
                    console.warn('Legacy workspace migration was skipped:', error);
                    showToast('Could not migrate an older local snapshot. It was kept unchanged.', 'info');
                }
            }
            if (!defaultWorkspace) {
                try {
                    defaultWorkspace = await workspaceStore.createWorkspace({
                        id: DEFAULT_WORKSPACE_ID,
                        name: DEFAULT_WORKSPACE_NAME,
                        format: 'tinysql-gob',
                    });
                } catch (error) {
                    // Another tab can create the initial workspace between
                    // our lookup and create request.
                    if (error?.code !== 'WORKSPACE_EXISTS') throw error;
                    defaultWorkspace = await workspaceStore.getWorkspace(DEFAULT_WORKSPACE_ID);
                }
            }
        }
        let workspace = defaultWorkspace;
        const rememberedID = storageGet(ACTIVE_WORKSPACE_KEY);
        if (rememberedID && rememberedID !== defaultWorkspace.id) {
            try {
                workspace = await workspaceStore.getWorkspace(rememberedID);
            } catch (error) {
                if (error?.code !== 'WORKSPACE_NOT_FOUND') throw error;
                storageRemove(ACTIVE_WORKSPACE_KEY);
            }
        }
        activeWorkspaceId = workspace.id;
        activeWorkspaceName = workspace.name;
        storageSet(ACTIVE_WORKSPACE_KEY, activeWorkspaceId);
        await refreshWorkspaceSelector();
        return true;
    } catch (error) {
        workspaceStore = null;
        console.warn('IndexedDB workspace storage is unavailable:', error);
        updateStatus('Local workspace storage unavailable');
        return false;
    }
}

async function saveDatabaseSnapshotNow() {
    if (!wasmReady || (
        typeof wasmApi.exportDatabaseBytes !== 'function'
        && typeof wasmApi.exportDatabase !== 'function'
    ) || !workspaceStore) {
        return false;
    }
    const revisionAtStart = snapshotRevision;
    const workspaceIdAtStart = activeWorkspaceId;
    const workspaceNameAtStart = activeWorkspaceName;
    const editor = document.getElementById('queryEditor');
    const metadataAtStart = {
        lastQuery: editor?.value || '',
        tableCount: currentTables.length,
        savedAt: new Date().toISOString(),
    };
    try {
        const exported = await exportWorkspaceSnapshot();
        const workspace = await workspaceStore.saveWorkspace(
            workspaceIdAtStart,
            exported.snapshot,
            {
                keepGenerations: 3,
                format: exported.format,
                metadata: metadataAtStart,
            }
        );
        // The snapshot is deliberately committed to the workspace that was
        // active when export started. A concurrent transition must never put
        // an old database generation into the newly active workspace.
        if (activeWorkspaceId === workspaceIdAtStart) {
            activeWorkspaceName = workspace.name || workspaceNameAtStart;
            snapshotDirty = snapshotRevision !== revisionAtStart;
            await refreshWorkspaceSelector();
            if (snapshotDirty) scheduleDatabaseSnapshotSave(0);
        }
        return true;
    } catch (error) {
        console.warn('Database snapshot export failed:', error);
        return false;
    }
}

function scheduleDatabaseSnapshotSave(delay = 250) {
    snapshotDirty = true;
    snapshotRevision += 1;
    window.clearTimeout(snapshotSaveTimer);
    if (snapshotIdleHandle !== null && typeof window.cancelIdleCallback === 'function') {
        window.cancelIdleCallback(snapshotIdleHandle);
        snapshotIdleHandle = null;
    }
    snapshotSaveTimer = window.setTimeout(() => {
        const save = () => {
            snapshotIdleHandle = null;
            if (snapshotDirty) {
                saveDatabaseSnapshotNow().catch((error) => console.warn('Autosave failed:', error));
            }
        };
        if (typeof window.requestIdleCallback === 'function') {
            snapshotIdleHandle = window.requestIdleCallback(save, { timeout: 2000 });
        } else {
            save();
        }
    }, delay);
}

async function restoreDatabaseSnapshot() {
    if ((typeof wasmApi.importDatabase !== 'function' && typeof wasmApi.importDatabaseBytes !== 'function') || !workspaceStore) return false;
    try {
        const restored = await workspaceStore.recoverWorkspace(activeWorkspaceId, {
            validate: async (snapshot) => {
                const result = await validateWorkspaceSnapshot(snapshot);
                return Boolean(result?.success);
            },
        });
        if (!restored.snapshot) return false;
        const imported = await importWorkspaceSnapshot(restored.snapshot);
        if (!imported?.success) {
            throw new Error(imported?.error || 'The validated workspace snapshot could not be restored.');
        }
        if (restored.recovered) {
            showToast('Recovered the most recent valid workspace version.', 'info');
        }
        // Validation and the subsequent selected-generation import both
        // succeeded, so only now is it safe to remove the old localStorage
        // copy.
        completeLegacySnapshotMigration(activeWorkspaceId, restored);
        updateStatus(`Restored workspace: ${activeWorkspaceName}`);
        return true;
    } catch (error) {
        console.warn('Workspace restore failed:', error);
        updateStatus(`Saved workspace could not be restored: ${error.message}`);
        return false;
    }
}

async function refreshWorkspaceSelector() {
    if (!workspaceStore) return [];
    const workspaces = await workspaceStore.listWorkspaces();
    const selector = document.getElementById('workspaceSelect');
    if (selector) {
        selector.innerHTML = workspaces.map((workspace) => {
            const selected = workspace.id === activeWorkspaceId ? ' selected' : '';
            const detail = `${formatRuntimeBytes(workspace.sizeBytes)} · ${new Date(workspace.updatedAt).toLocaleString()}`;
            return `<option value="${escapeHtml(workspace.id)}"${selected} title="${escapeHtml(detail)}">${escapeHtml(workspace.name)}</option>`;
        }).join('');
        selector.disabled = isWorkspaceTransitioning() || workspaces.length === 0;
    }
    renderWorkspacePanel(workspaces);
    return workspaces;
}

let workspacePanelDelegationReady = false;
function setupWorkspacePanelDelegation() {
    if (workspacePanelDelegationReady) return;
    const list = document.getElementById('workspaceList');
    if (!list) return;
    workspacePanelDelegationReady = true;
    list.addEventListener('click', (event) => {
        const button = event.target.closest('[data-workspace-action="open"]');
        const item = button?.closest('[data-workspace-id]');
        if (!item) return;
        switchWorkspace(item.dataset.workspaceId).then((switched) => {
            if (switched) closeWorkspacePanel();
        }).catch((error) => {
            console.error('Could not open workspace:', error);
            showToast(`Could not open workspace: ${error.message}`, 'error');
        });
    });
}

function renderWorkspacePanel(workspaces = []) {
    const list = document.getElementById('workspaceList');
    if (!list) return;
    setupWorkspacePanelDelegation();
    const transitionActive = isWorkspaceTransitioning();
    list.innerHTML = workspaces.length ? workspaces.map((workspace) => {
        const active = workspace.id === activeWorkspaceId ? ' active' : '';
        const updatedAt = new Date(workspace.updatedAt).toLocaleString();
        return `
            <div class="workspace-item${active}" data-workspace-id="${escapeHtml(workspace.id)}">
                <div class="workspace-item-copy">
                    <strong>${escapeHtml(workspace.name)}</strong>
                    <span>${formatRuntimeBytes(workspace.sizeBytes)} · ${escapeHtml(updatedAt)}</span>
                </div>
                <button type="button" data-workspace-action="open"${active || transitionActive ? ' disabled' : ''}>${active ? 'Current' : 'Open'}</button>
            </div>`;
    }).join('') : '<p class="workspace-empty">No workspaces yet.</p>';
}

async function openWorkspacePanel() {
    if (isWorkspaceTransitioning()) return;
    const panel = document.getElementById('workspacePanel');
    if (!panel) return;
    try {
        await refreshWorkspaceSelector();
    } catch (error) {
        showToast(`Could not list workspaces: ${error.message}`, 'error');
        return;
    }
    panel.classList.remove('hidden');
    panel.setAttribute('aria-hidden', 'false');
    document.getElementById('workspaceNameInput')?.focus();
}

function closeWorkspacePanel() {
    const panel = document.getElementById('workspacePanel');
    if (!panel) return;
    panel.classList.add('hidden');
    panel.setAttribute('aria-hidden', 'true');
}

async function switchWorkspace(workspaceId) {
    const nextId = String(workspaceId || '');
    if (!workspaceStore || !nextId || nextId === activeWorkspaceId) return false;
    const switched = await runWorkspaceTransition(async () => {
        try {
            const saved = await saveDatabaseSnapshotNow();
            if (!saved) {
                showToast('The current workspace could not be saved. It remains open.', 'error');
                return false;
            }
            const workspace = await workspaceStore.getWorkspace(nextId);
            updateStatus(`Opening workspace: ${workspace.name}…`);
            const restored = await workspaceStore.recoverWorkspace(nextId, {
                validate: async (snapshot) => {
                    const result = await validateWorkspaceSnapshot(snapshot);
                    return Boolean(result?.success);
                },
            });
            if (!restored.snapshot) {
                const cleared = await wasmApi.clearDatabase();
                if (!cleared?.success) throw new Error(cleared?.error || 'Could not prepare an empty workspace.');
            } else {
                const imported = await importWorkspaceSnapshot(restored.snapshot);
                if (!imported?.success) {
                    throw new Error(imported?.error || 'The validated workspace snapshot could not be restored.');
                }
            }
            activeWorkspaceId = workspace.id;
            activeWorkspaceName = workspace.name;
            workspaceEpoch += 1;
            storageSet(ACTIVE_WORKSPACE_KEY, activeWorkspaceId);
            completeLegacySnapshotMigration(activeWorkspaceId, restored);
            currentTables = [];
            resetWorkspaceResultView({ showIntro: !restored.snapshot });
            const editor = document.getElementById('queryEditor');
            if (editor) {
                editor.value = typeof workspace.metadata?.lastQuery === 'string' ? workspace.metadata.lastQuery : '';
                syncEditorHighlight();
                saveEditorState();
            }
            await loadTables();
            await refreshWorkspaceSelector();
            updateStatus(`Opened workspace: ${workspace.name}`);
            return true;
        } catch (error) {
            console.error('Could not switch workspace:', error);
            showToast(`Could not open workspace: ${error.message}`, 'error');
            await refreshWorkspaceSelector();
            return false;
        }
    });
    if (!switched) {
        try {
            await refreshWorkspaceSelector();
        } catch (error) {
            console.warn('Could not reset the workspace selector:', error);
        }
    }
    return switched;
}

async function createWorkspacePrompt() {
    await openWorkspacePanel();
}

async function createWorkspaceFromPanel() {
    if (!workspaceStore || !wasmReady) {
        showToast('Workspaces are still initializing.', 'info');
        return;
    }
    const nameInput = document.getElementById('workspaceNameInput');
    const trimmedName = nameInput?.value.trim() || '';
    if (!trimmedName) {
        showToast('A workspace needs a name.', 'error');
        return;
    }
    return runWorkspaceTransition(async () => {
        try {
            const saved = await saveDatabaseSnapshotNow();
            if (!saved) {
                showToast('The current workspace could not be saved. No new workspace was created.', 'error');
                return false;
            }
            const workspace = await workspaceStore.createWorkspace({ name: trimmedName, format: 'tinysql-gob' });
            const cleared = await wasmApi.clearDatabase();
            if (!cleared?.success) throw new Error(cleared?.error || 'Could not create an empty workspace.');
            activeWorkspaceId = workspace.id;
            activeWorkspaceName = workspace.name;
            workspaceEpoch += 1;
            storageSet(ACTIVE_WORKSPACE_KEY, activeWorkspaceId);
            currentTables = [];
            resetWorkspaceResultView({ showIntro: true });
            const editor = document.getElementById('queryEditor');
            if (editor) {
                editor.value = '';
                syncEditorHighlight();
                saveEditorState();
            }
            renderTables();
            await refreshWorkspaceSelector();
            if (nameInput) nameInput.value = '';
            closeWorkspacePanel();
            updateStatus(`Created workspace: ${workspace.name}`);
            return true;
        } catch (error) {
            console.error('Could not create workspace:', error);
            showToast(`Could not create workspace: ${error.message}`, 'error');
            return false;
        }
    });
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

// The legacy path is still the compatible choice for real SQL scripts. A
// trailing semicolon alone must not opt a single large SELECT out of the
// bounded ResultStream path, though, so count statement separators outside
// strings and comments instead of using query.includes(';').
function hasMultipleSQLStatements(sql) {
    const input = String(sql || '');
    let statements = 0;
    let hasContent = false;
    let quote = '';
    let lineComment = false;
    let blockComment = false;

    for (let index = 0; index < input.length; index += 1) {
        const char = input[index];
        const next = input[index + 1];

        if (lineComment) {
            if (char === '\n') lineComment = false;
            continue;
        }
        if (blockComment) {
            if (char === '*' && next === '/') {
                blockComment = false;
                index += 1;
            }
            continue;
        }
        if (quote) {
            hasContent = true;
            if (char === quote) {
                // SQL escapes quote characters by doubling them.
                if (next === quote) {
                    index += 1;
                } else {
                    quote = '';
                }
            }
            continue;
        }
        if (char === '-' && next === '-') {
            lineComment = true;
            index += 1;
            continue;
        }
        if (char === '/' && next === '*') {
            blockComment = true;
            index += 1;
            continue;
        }
        if (char === "'" || char === '"' || char === '`') {
            quote = char;
            hasContent = true;
            continue;
        }
        if (char === ';') {
            if (hasContent) {
                statements += 1;
                if (statements > 1) return true;
            }
            hasContent = false;
            continue;
        }
        if (!/\s/.test(char)) hasContent = true;
    }
    return statements > 0 && hasContent;
}

function setQueryCancellationState(active, cancelling = false) {
    const cancelButton = document.getElementById('cancelQueryBtn');
    if (!cancelButton) return;
    cancelButton.hidden = !active;
    cancelButton.disabled = !active || cancelling;
    cancelButton.textContent = cancelling ? 'Cancelling…' : '■ Cancel';
}

function cancelActiveQuery() {
    if (!queryExecutionInFlight || !activeQueryAbortController) return;
    setQueryCancellationState(true, true);
    activeQueryAbortController.abort();
    updateStatus('Cancelling streamed query…');
}

function scheduleStreamingPreviewRender() {
    if (streamPreviewRenderTimer) return;
    streamPreviewRenderTimer = setTimeout(() => {
        streamPreviewRenderTimer = null;
        if (!queryExecutionInFlight || !currentResults?.streamingInProgress) return;
        void renderResults(currentResults).catch((error) => {
            console.warn('Could not render streamed preview:', error);
        });
    }, 120);
}

function handleQueryStreamEvent(message) {
    if (!queryExecutionInFlight || !message?.event) return;
    const event = message.event;
    activeQueryStreamProgress = event;
    const phase = event.phase || 'progress';
    if (phase === 'cancelling') {
        updateStatus('Cancelling streamed query…');
        return;
    }

    if (phase === 'started') {
        const columns = Array.isArray(event.columns) ? event.columns.map((column) => String(column)) : [];
        currentResults = {
            columns,
            rows: [],
            rowCount: 0,
            filteredRowCount: 0,
            pageOffset: 0,
            serverPaged: false,
            pageKey: '',
            duration: 'Streaming…',
            streamed: true,
            previewOnly: true,
            streamingInProgress: true,
            truncated: false,
            rowsScanned: 0,
            rowsProduced: 0,
            resultBytes: 0,
            materialized: Boolean(event.materialized),
        };
        const resultsContainer = document.getElementById('resultsContainer');
        if (resultsContainer) {
            resultsContainer.innerHTML = `
                <div class="empty-state">
                    <div class="empty-state-icon">⏳</div>
                    <div class="empty-state-title">Streaming result</div>
                    <div class="empty-state-text">Rows will appear as they are scanned.</div>
                </div>
            `;
        }
    }

    if (phase === 'chunk' && currentResults?.streamingInProgress) {
        const rows = Array.isArray(event.rows) ? event.rows : [];
        if (rows.length) currentResults.rows.push(...rows);
        currentResults.rowCount = currentResults.rows.length;
        currentResults.filteredRowCount = currentResults.rows.length;
        currentResults.duration = `${Number(event.elapsedMs || 0).toFixed(1)} ms • streaming`;
        scheduleStreamingPreviewRender();
    }

    if (phase !== 'started' && phase !== 'progress' && phase !== 'chunk') return;

    const retained = Number(event.rowsRetained) || 0;
    const scanned = Number(event.rowsScanned) || 0;
    const produced = Number(event.rowsProduced) || 0;
    if (currentResults?.streamingInProgress) {
        currentResults.rowsScanned = scanned;
        currentResults.rowsProduced = produced;
        currentResults.resultBytes = Number(event.resultBytes) || 0;
        currentResults.materialized = Boolean(event.materialized);
    }
    const detail = event.materialized
        ? 'materializing result'
        : 'streaming result';
    const progress = scanned > 0
        ? `${retained.toLocaleString()} kept • ${scanned.toLocaleString()} scanned`
        : `${Math.max(retained, produced).toLocaleString()} rows`;
    updateStatus(`Query ${detail}: ${progress}`);

    const executeButton = document.getElementById('executeBtn');
    if (executeButton) {
        executeButton.textContent = `${event.materialized ? '⏳ Materializing' : '⏳ Streaming'}…`;
    }
}

// Initialize WASM
async function initWasm() {
    const statusIndicator = document.querySelector('.status-indicator');
    statusIndicator?.classList.remove('ready', 'failed');
    statusIndicator?.classList.add('loading');
    updateStatus('Loading local engine…');

    try {
        await initializeWorkspaceStorage();
        if (typeof globalThis.TinySQLWasmClient !== 'function') {
            throw new Error('WASM worker client did not load.');
        }
        wasmEngine = new globalThis.TinySQLWasmClient({
            workerUrl: 'wasm-worker.js',
            wasmUrl: 'query_files.wasm',
            wasmExecUrl: 'wasm_exec.js',
            preferCompressed: true,
        });
        wasmEngine.on('progress', (progress) => {
            if (progress.phase === 'queued' || progress.phase === 'started') {
                activeWorkerRequests.set(progress.requestId, progress.method);
            }
            if (progress.phase === 'completed' || progress.phase === 'failed' || progress.phase === 'cancelled') {
                activeWorkerRequests.delete(progress.requestId);
            }
            setRuntimeActivityHint(activeWorkerRequests.size);
        });
        wasmEngine.on('status', (status) => {
            if (status?.success) renderRuntimeStatus(status);
        });
        wasmEngine.on('stream', (message) => {
            handleQueryStreamEvent(message);
        });
        wasmEngine.on('error', (error) => {
            if (!wasmReady) return;
            wasmReady = false;
            activeWorkerRequests.clear();
            setRuntimeActivityHint(0);
            document.getElementById('executeBtn').disabled = true;
            statusIndicator?.classList.remove('loading', 'ready');
            statusIndicator?.classList.add('failed');
            updateStatus('Local engine stopped unexpectedly');
            showToast(`The local engine stopped: ${error.message || error}`, 'error');
        });
        await wasmEngine.init();
        for (const method of Object.keys(wasmApi)) {
            if (wasmEngine.supports(method) && typeof wasmEngine[method] === 'function') {
                wasmApi[method] = wasmEngine[method].bind(wasmEngine);
            }
        }
        wasmReady = true;
        console.log('WASM worker initialized successfully');

        if (typeof wasmApi.setRuntimeIdentity === 'function') {
            await wasmApi.setRuntimeIdentity('local-browser', getRuntimeSessionId());
        }

        console.log("Available WASM functions:", Object.fromEntries(
            Object.entries(wasmApi).map(([k,v]) => [k, typeof v])
        ));
        
        updateStatus("Ready");
        statusIndicator?.classList.remove('loading', 'failed');
        statusIndicator?.classList.add('ready');
        document.getElementById('executeBtn').disabled = false;
        await refreshRuntimeStatus();
        scheduleRuntimeStatusRefresh();
        const hashDemoPayload = decodeDemoHash();
        if (!hashDemoPayload) {
            await restoreDatabaseSnapshot();
        }
        // If any tables were registered client-side before WASM was ready,
        // import them now into the WASM-backed database so queries will work.
        if (Object.keys(pendingClientTables).length > 0) {
            console.log('Importing pending client tables into WASM:', Object.keys(pendingClientTables));
            for (const [tableName, rows] of Object.entries(pendingClientTables)) {
                try {
                    const jsonContent = JSON.stringify(rows);
                    const result = await wasmApi.importFile(`${tableName}.json`, jsonContent, tableName);
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
        await loadTables();
        if (hashDemoPayload) {
            await applyHashDemoPayload(hashDemoPayload);
            lastAppliedHash = window.location.hash || '';
        }
    } catch (err) {
        console.error("Failed to load WASM:", err);
        updateStatus("Failed to load WASM");
        statusIndicator?.classList.remove('loading', 'ready');
        statusIndicator?.classList.add('failed');
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

const DEMO_AI_DOCS_SQL = `DROP TABLE IF EXISTS ai_docs;
CREATE TABLE ai_docs (
    id INT PRIMARY KEY,
    title TEXT,
    category TEXT,
    content TEXT,
    embedding VECTOR
);
INSERT INTO ai_docs VALUES
    (1, 'Vector Search', 'ai', 'Vector search finds semantically similar records with embeddings and nearest-neighbor ranking.', '[1.0, 0.0, 0.0]'),
    (2, 'Full Text Search', 'search', 'Full text search ranks documents by matching query terms, phrases, and boolean expressions.', '[0.0, 1.0, 0.0]'),
    (3, 'Geo Analytics', 'geo', 'Geo analytics combines coordinates, distances, bounding boxes, and routing graph data.', '[0.0, 0.0, 1.0]'),
    (4, 'Hybrid Retrieval', 'ai', 'Hybrid retrieval combines full text ranking with vector similarity for RAG applications.', '[0.8, 0.2, 0.0]')`;

const DEMO_RAG_CHUNKS = [
    { doc_id: 'tinySQL', chunk_index: 0, tenant_id: 'public', chunk_text: 'tinySQL added browser-ready file analytics, query history, snapshots, and shareable URL hash demos.', quality: 0.78, created_at: '2026-08-24 10:00:00', geometry: '{"type":"Point","coordinates":[13.405,52.52]}', embedding: '[0.9, 0.2, 0.0]' },
    { doc_id: 'tinySQL', chunk_index: 1, tenant_id: 'public', chunk_text: 'Geodata imports now cover WKT, WKB, GeoPackageBinary, geohashes, reprojection, OGC TileMatrix and WMS axis ordering.', quality: 0.94, created_at: '2026-08-29 11:00:00', geometry: '{"type":"Point","coordinates":[11.5755,48.1372]}', embedding: '[1.0, 0.1, 0.1]' },
    { doc_id: 'tinySQL', chunk_index: 2, tenant_id: 'public', chunk_text: 'RAG helpers combine filtered vector search, spatial authorization, context expansion, warm indexes, and hybrid ranking.', quality: 0.96, created_at: '2026-08-30 12:00:00', geometry: '{"type":"Point","coordinates":[13.405,52.52]}', embedding: '[0.8, 0.6, 0.1]' },
    { doc_id: 'tinySQL', chunk_index: 3, tenant_id: 'private', chunk_text: 'Private roadmap notes cover streaming execution, vector update deltas, replication, and storage internals.', quality: 0.91, created_at: '2026-08-30 13:00:00', geometry: '{"type":"Point","coordinates":[8.6821,50.1109]}', embedding: '[0.4, 0.9, 0.2]' },
    { doc_id: 'ops', chunk_index: 0, tenant_id: 'private', chunk_text: 'Operational work added route graph warm-up, stored procedure scheduling, replica streaming, and WAL improvements.', quality: 0.86, created_at: '2026-08-30 14:00:00', geometry: '{"type":"Point","coordinates":[9.9937,53.5511]}', embedding: '[0.2, 0.4, 1.0]' }
];

const DEMO_RAG_CHUNKS_SQL = `DROP TABLE IF EXISTS rag_chunks;
CREATE TABLE rag_chunks (
    chunk_id INT PRIMARY KEY,
    doc_id TEXT,
    chunk_index INT,
    tenant_id TEXT,
    chunk_text TEXT,
    quality FLOAT,
    created_at TEXT,
    geometry GEOMETRY,
    embedding VECTOR
);
INSERT INTO rag_chunks VALUES
    (1, 'tinySQL', 0, 'public', 'tinySQL added browser-ready file analytics, query history, snapshots, and shareable URL hash demos.', 0.78, '2026-08-24 10:00:00', '{"type":"Point","coordinates":[13.405,52.52]}', '[0.9, 0.2, 0.0]'),
    (2, 'tinySQL', 1, 'public', 'Geodata imports now cover WKT, WKB, GeoPackageBinary, geohashes, reprojection, OGC TileMatrix and WMS axis ordering.', 0.94, '2026-08-29 11:00:00', '{"type":"Point","coordinates":[11.5755,48.1372]}', '[1.0, 0.1, 0.1]'),
    (3, 'tinySQL', 2, 'public', 'RAG helpers combine filtered vector search, spatial authorization, context expansion, warm indexes, and hybrid ranking.', 0.96, '2026-08-30 12:00:00', '{"type":"Point","coordinates":[13.405,52.52]}', '[0.8, 0.6, 0.1]'),
    (4, 'tinySQL', 3, 'private', 'Private roadmap notes cover streaming execution, vector update deltas, replication, and storage internals.', 0.91, '2026-08-30 13:00:00', '{"type":"Point","coordinates":[8.6821,50.1109]}', '[0.4, 0.9, 0.2]'),
    (5, 'ops', 0, 'private', 'Operational work added route graph warm-up, stored procedure scheduling, replica streaming, and WAL improvements.', 0.86, '2026-08-30 14:00:00', '{"type":"Point","coordinates":[9.9937,53.5511]}', '[0.2, 0.4, 1.0]')`;

const DEMO_RELEASE_FEATURES = [
    { area: 'Search/RAG', feature: 'Authorization and metadata pre-filters for RAG_SEARCH, HYBRID_SEARCH, VEC_SEARCH_FILTERED and FTS_SEARCH_FILTERED, including spatial bbox/radius filters', added: '2026-08-27 to 2026-08-30', browser_demo: 'Direct tenant + spatial pre-filter recipe over rag_chunks; filtering happens before ranking and context expansion' },
    { area: 'Search/RAG', feature: 'RAG_WARM builds the exact vector and lexical retrieval paths before the first user query', added: '2026-08-30', browser_demo: 'Direct RAG_WARM recipe reporting vector dimensions and FTS cache statistics' },
    { area: 'Routing', feature: 'ROUTE_WARM, A* shortest paths, distance matrices, reachable service areas, and faster versioned graph caches', added: '2026-08-27 to 2026-08-30', browser_demo: 'ROUTE_WARM + ROUTE_SHORTEST_PATH recipe over the imported routes_rg graph' },
    { area: 'Geodata', feature: 'WKT/EWKT and WKB/EWKB conversion, GeoPackageBinary inspection, geohashes, Web Mercator reprojection, ST_TOUCHES/ST_COVERS/ST_PERIMETER', added: '2026-08-29 to 2026-08-30', browser_demo: 'Direct OGC geometry interoperability recipe; path-based GeoPackage import remains native-only' },
    { area: 'Geodata', feature: 'CRS normalization, WMS 1.3 axis ordering, generic OGC TileMatrix bounds/positions, and TILE_COVER viewport enumeration', added: '2026-08-30', browser_demo: 'Direct CRS/WMS/TileMatrix recipe in the Geo query group' },
    { area: 'Execution', feature: 'Incremental ResultStream APIs and optimized streaming scans with configurable backpressure', added: '2026-08-26 to 2026-08-27', browser_demo: 'Single-statement browser queries use bounded row chunks with live progress and cancellation; scripts retain the materialized paging/export path' },
    { area: 'Performance', feature: 'Compiled-query, RAG, vector-update, routing, import/export and storage fast paths added through late August', added: '2026-08-12 to 2026-08-31', browser_demo: 'Used transparently; result view indexes now persist across page changes and snapshots are deferred to browser idle time' },
    { area: 'Networking', feature: 'Protocol Buffers and gRPC service support in cmd/server', added: '2026-08-31', browser_demo: 'Server-only; not linked into the local WASM playground' },
    { area: 'Performance', feature: 'RAG/FTS arena-backed document cache, parallel BM25 scan, subquery result cache, secondary-index skiplists, and ORDER BY/window-function sorts that avoid reflect.Swapper', added: '2026-07-26 to 2026-08-11', browser_demo: 'Invisible speed-up under existing SQL; nothing new to run' },
    { area: 'Security/Ops', feature: 'cmd/migrate incremental external-database sync and cmd/server replica mode', added: '2026-08-08', browser_demo: 'Go/CLI-only; server-side examples in cmd/migrate and cmd/server, not reachable from the browser build' },
    { area: 'Geodata', feature: 'MBTiles disk-backed tile serving (tinysqld -tiles HTTP endpoint, paged-index tile storage, direct tile-artifact import)', added: '2026-08-01 to 2026-08-06', browser_demo: 'Go/CLI/server-side; documented in browser feature matrix' },
    { area: 'Storage', feature: 'ModeSQLite backend: persist tinySQL tables as a native SQLite file via modernc.org/sqlite (-tags sqliteimport)', added: '2026-08-04', browser_demo: 'Go/CLI-only; excluded from the WASM build by build tag' },
    { area: 'Geodata', feature: 'GEO_DISSOLVE/GEO_UNION_AGG/ST_UNION, GEO_BBOX_AGG, GEO_CENTROID_AGG region aggregates and GEO_SEARCH indexed bbox/radius table search', added: '2026-08-04', browser_demo: 'Direct scalar/aggregate SQL and table function ("GEO_DISSOLVE Regions" and "GEO_SEARCH Bbox" recipes)' },
    { area: 'Geodata', feature: 'GEO_CLIP polygon clipping and GEO_INTERSECTS/GEO_DISJOINT/GEO_EQUALS geometry relations', added: '2026-08-04', browser_demo: 'Direct SQL ("GEO_CLIP & Relations" recipe)' },
    { area: 'Geodata', feature: 'EQUAL_INTERVAL / NATURAL_BREAKS choropleth classification window functions', added: '2026-08-04', browser_demo: 'Direct window functions ("Choropleth Classes" recipe); also demoed on the standalone tiles-demo-bavaria.html choropleth panel' },
    { area: 'Developer UX', feature: 'TopoJSON and XLSX import/export (internal/importer, internal/exporter)', added: '2026-08-04', browser_demo: 'Go API-only; the browser demo\'s .xlsx upload uses a client-side JS library instead, and there is no export option for either format' },
    { area: 'Geodata', feature: 'GEO_BUFFER, GEO_CONVEX_HULL, GEO_ENVELOPE, GEO_LINE_INTERPOLATE geometry construction', added: '2026-08-03', browser_demo: 'Direct SQL ("GEO_BUFFER Circle" and "GEO_CONVEX_HULL & Envelope" recipes); GEO_BUFFER also powers the tiles-demo-bavaria.html choropleth panel' },
    { area: 'Geodata', feature: 'GEO_CLEAN, GEO_SNAP/ST_SNAPTOGRID, GEO_IS_VALID geometry quality checks', added: '2026-08-03', browser_demo: 'Direct ST_CLEAN/ST_SNAPTOGRID/ST_ISVALID recipes on the tiles-demo.html Mapshaper-style editing panel' },
    { area: 'Geodata', feature: 'GEO_BBOX/GEO_CENTROID/GEO_AFFINE/GEO_SMOOTH/GEO_DROP_HOLES editing and GEO_SIMPLIFY simplification', added: '2026-08-03', browser_demo: 'Direct ST_BBOX/ST_CENTROID/ST_AFFINE/ST_SMOOTH/ST_REMOVE_HOLES/ST_SIMPLIFY recipes on the tiles-demo.html Mapshaper-style editing panel' },
    { area: 'Geodata', feature: 'GEO_BEARING/ST_AZIMUTH, GEO_DESTINATION/ST_PROJECT, GEO_MIDPOINT, GEO_WITHIN_POLYGON/ST_CONTAINS, GEO_POLYGON_AREA, GEO_LENGTH', added: '2026-08-03', browser_demo: 'Direct SQL ("Route Bearing & Midpoint" and "GEO_DESTINATION & Area" recipes); GEO_BEARING/GEO_MIDPOINT also demoed on tiles-demo.html' },
    { area: 'Query planning', feature: 'Ordered range seeks on numeric secondary indexes (BETWEEN/one-sided comparisons now use the index instead of a table scan)', added: '2026-08-01', browser_demo: 'Visible only via EXPLAIN plan shape; no dedicated example' },
    { area: 'Analytics SQL', feature: 'Scientific-notation numeric literals (e.g. 1.25e-3) now parse', added: '2026-08-02', browser_demo: 'No dedicated example; usable in any numeric expression' },
    { area: 'Geodata', feature: 'TILE_X/TILE_Y/TILE_ZXY/TILE_FLIP_Y/TILE_LON/TILE_LAT/TILE_BBOX/TILE_QUADKEY/TILE_FROM_QUADKEY/TILE_PARENT/TILE_COUNT/TILE_CONTAINS tile-addressing functions', added: '2026-08-01', browser_demo: 'TILE_ZXY/TILE_FLIP_Y/TILE_BBOX/TILE_QUADKEY/TILE_PARENT/TILE_COUNT demoed on tiles-demo.html and tiles-demo-bavaria.html; TILE_X/TILE_Y/TILE_LON/TILE_LAT/TILE_FROM_QUADKEY/TILE_CONTAINS have no example anywhere' },
    { area: 'Developer UX', feature: 'BLOB_FROM_HEX/BLOB_FROM_BASE64/BLOB_SUBSTR/BLOB_CONCAT now return storable blobs instead of hex text, so they can be inserted directly into BLOB columns', added: '2026-08-01', browser_demo: 'Direct SQL ("BLOB Constructors" recipe)' },
    { area: 'Search/RAG', feature: 'HYBRID_SEARCH: one call fuses BM25 keyword and vector retrieval by reciprocal-rank fusion, given a search term and a query vector', added: '2026-07-29', browser_demo: 'Direct HYBRID_SEARCH table function ("HYBRID_SEARCH + Wildcards" recipe and the ai_docs default query)' },
    { area: 'Search/RAG', feature: 'RAG_SEARCH composed vector, keyword, and context retrieval', added: '2026-07-25', browser_demo: 'Direct RAG_SEARCH table function with reciprocal-rank fusion and neighbor expansion' },
    { area: 'Search/RAG', feature: 'CONTAINS_ALL, CONTAINS_ANY, CONTAINS_SCORE literal text matching', added: '2026-07-24', browser_demo: 'Direct case-insensitive filter and ranking over imported text' },
    { area: 'Vector math', feature: 'VEC_HAMMING_DISTANCE and VEC_CENTROID helpers', added: '2026-07-25', browser_demo: 'Direct browser-side binary-signature comparison and centroid calculation' },
    { area: 'Query planning', feature: 'ANALYZE and sys.statistics planner metadata', added: '2026-07-15', browser_demo: 'Direct ANALYZE plus sys.statistics introspection' },
    { area: 'Geodata', feature: 'GeoJSON importer', added: '2026-07-08', browser_demo: 'Direct upload/import and ST_* SQL examples' },
    { area: 'Geodata', feature: 'KML ExtendedData, SchemaData, MultiGeometry, altitude', added: '2026-07-08', browser_demo: 'Direct .kml import' },
    { area: 'Geodata', feature: 'OSM XML nodes, ways, relations, refs, geometry', added: '2026-07-08', browser_demo: 'Direct .osm/.osm.xml import' },
    { area: 'Geodata', feature: 'Routing graph JSON/CSV/NDJSON with node and edge tables', added: '2026-07-08', browser_demo: 'Direct .rg and .graph.json import' },
    { area: 'Geodata', feature: 'Shapefile ZIP and MBTiles metadata imports', added: '2026-07-08', browser_demo: 'Go/CLI/server-side; documented in browser feature matrix' },
    { area: 'Search/RAG', feature: 'FTS snippets, BM25 ranking, vector indexes, RAG context, hybrid scoring', added: '2026-05-10 to 2026-07-08', browser_demo: 'Direct SQL over ai_docs and rag_chunks' },
    { area: 'Analytics SQL', feature: 'CTE views, materialized views, PIVOT, RETURNING, EXPLAIN', added: '2026-06-21 to 2026-07-08', browser_demo: 'Direct multi-statement SQL recipes' },
    { area: 'Catalog', feature: 'sys.* metadata, dependencies, functions, procedures, SQLite-compatible PRAGMAs', added: '2026-06-21 to 2026-07-08', browser_demo: 'Direct catalog queries' },
    { area: 'Security/Ops', feature: 'RBAC, audit logs, encryption, WAL/storage, tinysqld, MCP server', added: '2026-05-14 to 2026-07-05', browser_demo: 'Feature matrix and metadata queries; server-side examples in Go tools' },
    { area: 'Developer UX', feature: 'tinyORM, public importer/resultutil/sqlutil/jobs/standards packages, SQL BeautifySQL/MinifySQL helpers, gh-pages workflow', added: '2026-07-05 to 2026-07-19', browser_demo: 'Go API docs are linked from the top bar' }
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
        title: 'Hybrid vector + wildcard search',
        description: 'One term drives semantic vector and wildcard-aware full-text retrieval, fused with reciprocal-rank fusion.',
        icon: '🧠',
        tables: ['ai_docs'],
        autoRun: true,
        query: `-- Hybrid search: ? matches one character, * matches many\nSELECT id, title, category, _vec_rank, _fts_rank, _rrf_rank, _rrf_score\nFROM HYBRID_SEARCH(\n    'ai_docs', 'embedding', 'content', 'vect?r* OR retrieval',\n    VEC_FROM_JSON('[1.0, 0.0, 0.0]'), 4,\n    '{"key_columns":["id"]}'\n)\nORDER BY _rrf_rank`
    },
    ragsearch: {
        title: 'Composed RAG search',
        description: 'New: one table-valued function combines vector retrieval, keyword fusion, and neighboring context chunks.',
        icon: '🧠',
        tables: ['rag_chunks'],
        autoRun: true,
        query: `-- New: RAG_SEARCH composes vector + text retrieval + context expansion\nSELECT doc_id, chunk_index, chunk_text, _hit_rank, _context_offset, _context_rank\nFROM RAG_SEARCH('rag_chunks', 'embedding', VEC_FROM_JSON('[0.8, 0.6, 0.1]'), 2, '{\n    "text_column": "chunk_text",\n    "text_query": "vector search RAG",\n    "key_columns": ["doc_id", "chunk_index"],\n    "expand_before": 1,\n    "expand_after": 1,\n    "doc_id_column": "doc_id",\n    "chunk_index_column": "chunk_index"\n}')\nORDER BY _context_rank`
    },
    spatialrag: {
        title: 'Filtered spatial RAG',
        description: 'Tenant and radius constraints are enforced before vector ranking, so forbidden or distant rows never enter the candidate set.',
        icon: '🛡️',
        tables: ['rag_chunks'],
        autoRun: true,
        query: `-- Authorization + location are applied before ranking\nSELECT chunk_id, doc_id, tenant_id, chunk_text, _vec_rank, _vec_distance\nFROM VEC_SEARCH_FILTERED(\n    'rag_chunks', 'embedding', VEC_FROM_JSON('[0.8,0.6,0.1]'), 5,\n    '{"pre_filter":{"equals":{"tenant_id":"public"},"spatial":{"geometry_column":"geometry","center":[13.405,52.52],"radius_meters":600000}}}'\n)\nORDER BY _vec_rank`
    },
    contains: {
        title: 'Literal multi-term search',
        description: 'New: case-insensitive CONTAINS_ALL, CONTAINS_ANY, and CONTAINS_SCORE for straightforward text filtering and ranking.',
        icon: '🔎',
        tables: ['ai_docs'],
        autoRun: true,
        query: `-- New: literal, case-insensitive multi-term matching (not LIKE wildcards)\nSELECT title, category,\n       CONTAINS_SCORE(content, 'vector', 'search', 'retrieval') AS matched_terms\nFROM ai_docs\nWHERE CONTAINS_ANY(content, 'vector', 'retrieval')\nORDER BY matched_terms DESC, title`
    },
    vectormath: {
        title: 'Binary vectors and centroids',
        description: 'New: compare compact binary signatures with Hamming distance and derive representative vectors with VEC_CENTROID.',
        icon: '📐',
        tables: ['ai_docs'],
        autoRun: true,
        query: `-- New: portable Hamming distance and centroid helpers\nSELECT title,\n       VEC_HAMMING_DISTANCE(VEC_BINARY_QUANTIZE(embedding), VEC_FROM_JSON('[1,0,0]')) AS hamming_distance,\n       VEC_CENTROID(embedding, VEC_FROM_JSON('[1,0,0]')) AS midpoint_vector\nFROM ai_docs\nORDER BY hamming_distance, title`
    },
    statistics: {
        title: 'Planner statistics',
        description: 'New: ANALYZE persists exact table statistics, visible through sys.statistics and used for index selection.',
        icon: '📈',
        tables: ['sales'],
        autoRun: true,
        query: `-- New: collect exact column statistics for the query planner\nANALYZE sales;\nSELECT column_name, row_count, distinct_count, null_count, min, max, is_stale\nFROM sys.statistics\nWHERE table_name = 'sales'\nORDER BY column_name`
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
    procedures: {
        title: 'Stored procedures',
        description: 'Typed arguments, safe nested SQL, read-only concurrency, atomic writes, runtime statistics, and reusable Go/WASM registrations.',
        icon: '⚙️',
        tables: [],
        autoRun: true,
        query: `-- Berlin to Munich: a read-only procedure can run concurrently\nCALL demo_geo_distance(52.5200, 13.4050, 48.1372, 11.5755)`
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
        ai_docs: `SELECT id, title, category, _vec_rank, _fts_rank, _rrf_rank, _rrf_score\nFROM HYBRID_SEARCH(\n    'ai_docs', 'embedding', 'content', 'vect?r* OR retrieval',\n    VEC_FROM_JSON('[1.0, 0.0, 0.0]'), 4,\n    '{"key_columns":["id"]}'\n)\nORDER BY _rrf_rank`,
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
    const payload = {
        name: tableName,
        fileName,
        content: typeof data === 'string' ? data : JSON.stringify(data),
    };
    if (demo.setupSQL) {
        payload.setupSQL = typeof demo.setupSQL === 'function' ? demo.setupSQL() : demo.setupSQL;
    }
    return payload;
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

function openStudio({ focusEditor = true } = {}) {
    document.body.classList.remove('landing-mode');
    document.body.classList.add('studio-mode');
    if (focusEditor) {
        window.setTimeout(() => document.getElementById('queryEditor')?.focus(), 0);
    }
}

function showLanding() {
    document.body.classList.remove('studio-mode');
    document.body.classList.add('landing-mode');
    window.scrollTo({ top: 0, behavior: 'smooth' });
}

window.openStudio = openStudio;
window.showLanding = showLanding;

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
    const starterDemoIDs = ['analytics', 'geo', 'rag', 'spatialrag'];
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
                    <h2>Run SQL on local files, without sending them anywhere.</h2>
                    <p class="intro-copy">
                        Add a file, write a query, and inspect the result. Everything runs in this browser — no account or backend required.
                    </p>
                    <div class="intro-actions">
                        <button onclick="showUploadDialog()">Add a file</button>
                        <button class="secondary" onclick="loadShareableDemo('analytics')">Try sample data</button>
                    </div>
                </div>
                <div class="intro-metrics">
                    <div class="intro-metric"><strong>1. Add data</strong><span>CSV, JSON, Excel, GeoJSON and more.</span></div>
                    <div class="intro-metric"><strong>2. Write SQL</strong><span>Autocomplete and formatting are built in.</span></div>
                    <div class="intro-metric"><strong>3. Export results</strong><span>Copy or download the result in common formats.</span></div>
                </div>
            </section>
            <section class="feature-strip">
                <div class="feature-pill"><strong>Files</strong>Typed local imports for everyday data work.</div>
                <div class="feature-pill"><strong>Maps</strong>Distance, radius and routing-graph SQL.</div>
                <div class="feature-pill"><strong>SQL features</strong>Views, PIVOT, windows and export-ready results.</div>
            </section>
            <section class="intro-grid">${cards}</section>
            <p class="intro-note">More examples appear in the data panel after loading a sample. Shareable links preserve both the query and its demo data.</p>
        </div>
    `;
}

function closeUtilityMenu() {
    const menu = document.getElementById('utilityMenu');
    const trigger = document.getElementById('toolsTrigger');
    menu?.classList.add('hidden');
    trigger?.setAttribute('aria-expanded', 'false');
}

function toggleUtilityMenu() {
    const menu = document.getElementById('utilityMenu');
    const trigger = document.getElementById('toolsTrigger');
    if (!menu || !trigger) return;
    const isOpening = menu.classList.contains('hidden');
    menu.classList.toggle('hidden', !isOpening);
    trigger.setAttribute('aria-expanded', String(isOpening));
}

function toggleRuntimePanel(force) {
    const panel = document.getElementById('runtimePanel');
    const button = document.getElementById('runtimeStatusButton');
    if (!panel) return;
    runtimePanelOpen = typeof force === 'boolean' ? force : panel.classList.contains('hidden');
    panel.classList.toggle('hidden', !runtimePanelOpen);
    panel.setAttribute('aria-hidden', runtimePanelOpen ? 'false' : 'true');
    button?.setAttribute('aria-expanded', runtimePanelOpen ? 'true' : 'false');
    if (runtimePanelOpen) refreshRuntimeStatus();
    scheduleRuntimeStatusRefresh();
}

function scheduleRuntimeStatusRefresh() {
    window.clearTimeout(runtimeRefreshTimer);
    runtimeRefreshTimer = window.setTimeout(async () => {
        await refreshRuntimeStatus();
        scheduleRuntimeStatusRefresh();
    }, runtimePanelOpen ? 1000 : 5000);
}

function formatRuntimeBytes(value) {
    const bytes = Number(value) || 0;
    if (bytes < 1024) return `${bytes} B`;
    if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
    return `${(bytes / 1024 / 1024).toFixed(1)} MB`;
}

function formatRuntimeDuration(seconds) {
    const total = Math.max(0, Number(seconds) || 0);
    if (total < 60) return `${Math.floor(total)}s`;
    if (total < 3600) return `${Math.floor(total / 60)}m ${Math.floor(total % 60)}s`;
    return `${Math.floor(total / 3600)}h ${Math.floor((total % 3600) / 60)}m`;
}

function setRuntimeActivityHint(active) {
    const button = document.getElementById('runtimeStatusButton');
    const activeElement = document.getElementById('runtimeActiveCompact');
    if (activeElement) activeElement.textContent = `${active} active`;
    const activeMetric = document.getElementById('runtimeActiveValue');
    const loadMetric = document.getElementById('runtimeLoadValue');
    if (activeMetric) activeMetric.textContent = String(active);
    if (loadMetric) loadMetric.textContent = active > 0 ? 'Busy' : 'Idle';
    button?.classList.toggle('busy', active > 0);
}

async function refreshRuntimeStatus() {
    if (!wasmReady || typeof wasmApi.getRuntimeStatus !== 'function') return;
    if (runtimeStatusRequest) return runtimeStatusRequest;
    runtimeStatusRequest = (async () => {
        try {
            const status = await wasmApi.getRuntimeStatus();
            if (status?.success) renderRuntimeStatus(status);
        } catch (error) {
            console.warn('Runtime status unavailable:', error);
        } finally {
            runtimeStatusRequest = null;
        }
    })();
    return runtimeStatusRequest;
}

function renderRuntimeStatus(status) {
    const requests = status.requests || {};
    const performance = status.performance || {};
    const memory = status.memory || {};
    const database = status.database || {};
    const identity = status.identity || {};
    const active = Math.max(Number(requests.active) || 0, activeWorkerRequests.size);
    setRuntimeActivityHint(active);
    const compactMemory = document.getElementById('runtimeMemoryCompact');
    if (compactMemory) compactMemory.textContent = formatRuntimeBytes(memory.heapAllocBytes);

    const values = {
        runtimeLoadValue: active > 0 ? 'Busy' : 'Idle',
        runtimeActiveValue: String(active),
        runtimeTotalValue: String(Number(requests.total) || 0),
        runtimeFailedValue: String(Number(requests.failed) || 0),
        runtimeThroughputValue: `${Number(requests.completedLastMinute) || 0}/min`,
        runtimePeakValue: String(Number(requests.peakActive) || 0),
        runtimeHeapValue: formatRuntimeBytes(memory.heapAllocBytes),
        runtimeHeapSystemValue: formatRuntimeBytes(memory.heapSystemBytes),
        runtimeTablesValue: String(Number(database.tables) || 0),
        runtimeRowsValue: String(Number(database.rows) || 0),
        runtimeCacheValue: `${Number(database.queryCacheEntries) || 0}/${Number(database.queryCacheCapacity) || 0}`,
        runtimeUptimeValue: formatRuntimeDuration(status.uptimeSeconds),
        runtimeUserValue: String(identity.userId || 'local-browser'),
        runtimeTenantValue: String(identity.tenant || 'default'),
        runtimeSessionValue: String(identity.sessionId || '—'),
        runtimeDistinctUsersValue: String(Number(identity.distinctUsers) || 0),
        runtimeAverageValue: `${(Number(performance.averageDurationMs) || 0).toFixed(2)} ms`,
        runtimeLastValue: `${(Number(performance.lastDurationMs) || 0).toFixed(2)} ms`,
        runtimeBusyShareValue: `${(Number(performance.busyPercentSinceStart) || 0).toFixed(1)}%`,
    };
    for (const [id, value] of Object.entries(values)) {
        const element = document.getElementById(id);
        if (element) element.textContent = value;
    }

    const kindBody = document.getElementById('runtimeKindRows');
    if (kindBody) {
        const kinds = Object.entries(requests.byKind || {}).sort(([left], [right]) => left.localeCompare(right));
        kindBody.innerHTML = kinds.length ? kinds.map(([kind, counters]) => `
            <tr><td>${escapeHtml(kind)}</td><td>${Number(counters.active) || 0}</td><td>${Number(counters.total) || 0}</td><td>${Number(counters.failed) || 0}</td><td>${Number(counters.timedOut) || 0}</td></tr>
        `).join('') : '<tr><td colspan="5">No requests yet</td></tr>';
    }

    const userBody = document.getElementById('runtimeUserRows');
    if (userBody) {
        const users = Array.isArray(status.users) ? status.users : [];
        userBody.innerHTML = users.length ? users.map((user) => `
            <tr><td>${escapeHtml(user.userId)}</td><td>${Number(user.sessions) || 0}</td><td>${Number(user.activeRequests) || 0}</td><td>${Number(user.totalRequests) || 0}</td><td>${Number(user.failedRequests) || 0}</td></tr>
        `).join('') : '<tr><td colspan="5">No users observed</td></tr>';
    }
}

function toggleDemoQueries() {
    const content = document.getElementById('demoQueryContent');
    const trigger = document.getElementById('demoQueriesToggle');
    if (!content || !trigger) return;
    const isOpening = content.classList.contains('hidden');
    content.classList.toggle('hidden', !isOpening);
    trigger.setAttribute('aria-expanded', String(isOpening));
}

function closeResultsExportMenu() {
    const menu = document.getElementById('resultsExportMenu');
    const trigger = document.getElementById('resultsExportTrigger');
    menu?.classList.add('hidden');
    trigger?.setAttribute('aria-expanded', 'false');
}

function toggleResultsExportMenu() {
    const menu = document.getElementById('resultsExportMenu');
    const trigger = document.getElementById('resultsExportTrigger');
    if (!menu || !trigger) return;
    const isOpening = menu.classList.contains('hidden');
    menu.classList.toggle('hidden', !isOpening);
    trigger.setAttribute('aria-expanded', String(isOpening));
}

let generatedCodeState = null;
const PYTHON_GENERATOR_RESERVED = new Set(['and', 'as', 'assert', 'async', 'await', 'break', 'class', 'continue', 'def', 'del', 'elif', 'else', 'except', 'False', 'finally', 'for', 'from', 'global', 'if', 'import', 'in', 'is', 'lambda', 'None', 'nonlocal', 'not', 'or', 'pass', 'raise', 'return', 'True', 'try', 'while', 'with', 'yield']);

function closeCodeGeneratorMenu() {
    const menu = document.getElementById('codeGeneratorMenu');
    const trigger = document.getElementById('codeGeneratorTrigger');
    menu?.classList.add('hidden');
    trigger?.setAttribute('aria-expanded', 'false');
}

function toggleCodeGeneratorMenu() {
    const menu = document.getElementById('codeGeneratorMenu');
    const trigger = document.getElementById('codeGeneratorTrigger');
    if (!menu || !trigger) return;
    const isOpening = menu.classList.contains('hidden');
    closeResultsExportMenu();
    menu.classList.toggle('hidden', !isOpening);
    trigger.setAttribute('aria-expanded', String(isOpening));
}

function generatedIdentifier(column, style, fallbackIndex) {
    const parts = String(column || '').replace(/[^\p{L}\p{N}]+/gu, ' ').trim().split(/\s+/u).filter(Boolean);
    const normalized = parts.map((part) => part.toLocaleLowerCase());
    let name = style === 'pascal'
        ? normalized.map((part) => part.charAt(0).toLocaleUpperCase() + part.slice(1)).join('')
        : normalized.join('_');
    if (!name || !/^[A-Za-z_]/u.test(name)) name = `field_${fallbackIndex + 1}`;
    if (style === 'snake' && PYTHON_GENERATOR_RESERVED.has(name)) name = `${name}_`;
    return name;
}

function uniqueGeneratedIdentifiers(columns, style) {
    const used = new Map();
    return columns.map((column, index) => {
        const base = generatedIdentifier(column, style, index);
        const seen = used.get(base) || 0;
        used.set(base, seen + 1);
        return seen ? `${base}_${seen + 1}` : base;
    });
}

function inferGeneratedType(rows, column) {
    const values = rows.map((row) => row?.[column]);
    const hasNull = values.some((value) => value === null || value === undefined);
    const kinds = new Set();
    for (const value of values) {
        if (value === null || value === undefined) continue;
        if (typeof value === 'number') kinds.add(Number.isInteger(value) ? 'integer' : 'number');
        else if (typeof value === 'boolean') kinds.add('boolean');
        else if (typeof value === 'string') kinds.add('string');
        else kinds.add('unknown');
    }
    if (kinds.size === 0) kinds.add('unknown');
    if (kinds.has('integer') && kinds.has('number')) {
        kinds.delete('integer');
        kinds.add('number');
    }
    return { kinds: [...kinds], hasNull };
}

function generatedType(info, language) {
    const kind = info.kinds.length === 1 ? info.kinds[0] : 'unknown';
    const types = {
        go: { integer: 'int64', number: 'float64', boolean: 'bool', string: 'string', unknown: 'any' },
        typescript: { integer: 'number', number: 'number', boolean: 'boolean', string: 'string', unknown: 'unknown' },
        python: { integer: 'int', number: 'float', boolean: 'bool', string: 'str', unknown: 'Any' },
        sql: { integer: 'BIGINT', number: 'DOUBLE PRECISION', boolean: 'BOOLEAN', string: 'TEXT', unknown: 'TEXT' },
    };
    const type = types[language]?.[kind] || types[language]?.unknown || 'unknown';
    if (language === 'typescript' && info.hasNull) return `${type} | null`;
    if (language === 'python' && info.hasNull) return `Optional[${type}]`;
    return type;
}

function generateResultCode(language, visible = getVisibleResults()) {
    if (!visible?.columns?.length || !Array.isArray(visible.rows)) return null;
    const columns = visible.columns;
    const rows = visible.rows;
    const types = columns.map((column) => inferGeneratedType(rows, column));
    const title = 'QueryResult';
    if (language === 'go') {
        const fields = uniqueGeneratedIdentifiers(columns, 'pascal');
        return { extension: 'go', label: 'Go struct', code: `// Generated from the current tinySQL result page.\ntype ${title} struct {\n${columns.map((column, index) => `\t${fields[index]} ${generatedType(types[index], 'go')} \`json:${JSON.stringify(column)}\``).join('\n')}\n}\n` };
    }
    if (language === 'typescript') {
        return { extension: 'ts', label: 'TypeScript interface', code: `// Generated from the current tinySQL result page.\nexport interface ${title} {\n${columns.map((column, index) => `  ${JSON.stringify(column)}: ${generatedType(types[index], 'typescript')};`).join('\n')}\n}\n` };
    }
    if (language === 'python') {
        const fields = uniqueGeneratedIdentifiers(columns, 'snake');
        const needsOptional = types.some((type) => type.hasNull);
        const needsAny = types.some((type) => type.kinds.length !== 1 || type.kinds[0] === 'unknown');
        const imports = ['from dataclasses import dataclass'];
        if (needsOptional || needsAny) imports.push(`from typing import ${[needsAny ? 'Any' : '', needsOptional ? 'Optional' : ''].filter(Boolean).join(', ')}`);
        return { extension: 'py', label: 'Python dataclass', code: `${imports.join('\n')}\n\n# Generated from the current tinySQL result page.\n@dataclass\nclass ${title}:\n${columns.map((column, index) => `    ${fields[index]}: ${generatedType(types[index], 'python')}  # ${String(column).replace(/[\r\n]+/gu, ' ')}`).join('\n')}\n` };
    }
    if (language === 'sql') {
        return { extension: 'sql', label: 'SQL CREATE TABLE', code: `-- Generated from the current tinySQL result page.\nCREATE TABLE query_result (\n${columns.map((column, index) => `  ${quoteSqlIdentifier(column)} ${generatedType(types[index], 'sql')}${types[index].hasNull ? '' : ' NOT NULL'}${index === columns.length - 1 ? '' : ','}`).join('\n')}\n);\n` };
    }
    return null;
}

function showGeneratedCode(language) {
    const generated = generateResultCode(language);
    if (!generated) {
        showToast('Run a query with rows before generating code.', 'info');
        return;
    }
    generatedCodeState = generated;
    closeCodeGeneratorMenu();
    let panel = document.getElementById('codeGeneratorPanel');
    if (!panel) {
        panel = document.createElement('section');
        panel.id = 'codeGeneratorPanel';
        panel.className = 'code-generator-panel';
        document.getElementById('resultsContainer')?.prepend(panel);
    }
    panel.innerHTML = `<div class="code-generator-header"><div><strong>${escapeHtml(generated.label)}</strong><span>Inferred from ${getVisibleResults().rows.length} visible row(s)</span></div><div><button type="button" onclick="copyGeneratedCode()">Copy</button><button type="button" onclick="downloadGeneratedCode()">Download .${escapeHtml(generated.extension)}</button><button type="button" onclick="closeGeneratedCode()" aria-label="Close generated code">✕</button></div></div><pre><code>${escapeHtml(generated.code)}</code></pre>`;
    panel.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
}

function closeGeneratedCode() {
    generatedCodeState = null;
    document.getElementById('codeGeneratorPanel')?.remove();
}

async function copyGeneratedCode() {
    if (!generatedCodeState) return;
    try {
        await copyTextToClipboard(generatedCodeState.code);
        showToast('Generated code copied', 'success');
    } catch (error) {
        showToast(`Could not copy generated code: ${error.message || error}`, 'error');
    }
}

function downloadGeneratedCode() {
    if (!generatedCodeState) return;
    downloadFile(generatedCodeState.code, `query_result.${generatedCodeState.extension}`, 'text/plain;charset=utf-8');
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
        getData: () => DEMO_AI_DOCS,
        setupSQL: DEMO_AI_DOCS_SQL
    },
    rag_chunks: {
        name: 'rag_chunks',
        fileName: 'rag_chunks.json',
        getData: () => DEMO_RAG_CHUNKS,
        setupSQL: DEMO_RAG_CHUNKS_SQL
    },
    release_features: {
        name: 'release_features',
        fileName: 'release_features.json',
        getData: () => DEMO_RELEASE_FEATURES
    }
};

// Load demo table
async function loadDemoTable(tableName) {
    if (!requireStableWorkspace('load demo data')) return;
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
                const result = demo.setupSQL && typeof wasmApi.executeMulti === 'function'
                    ? await wasmApi.executeMulti(demo.setupSQL)
                    : await wasmApi.importFile(fileName, fileContent, tableName);
                if (result && result.success) {
                    const schema = demo.setupSQL && typeof wasmApi.getTableSchema === 'function'
                        ? await wasmApi.getTableSchema(tableName)
                        : null;
                    const tableInfo = {
                        name: tableName,
                        rowCount: schema?.success ? schema.rows : result.rowsImported,
                        columns: schema?.success && Array.isArray(schema.columns)
                            ? schema.columns.map(c => typeof c === 'object' ? String(c.name) : String(c))
                            : (Array.isArray(result.columns) ? result.columns.map(c => String(c)) : [])
                    };
                    const existingIndex = currentTables.findIndex(t => t.name === tableName);
                    if (existingIndex >= 0) {
                        currentTables[existingIndex] = tableInfo;
                    } else {
                        currentTables.push(tableInfo);
                    }
                    renderTables();
                    if (isRoutingGraphFile(fileName)) {
                        await loadTables();
                    }
                    updateStatus(`Demo table "${tableName}" loaded: ${tableInfo.rowCount} rows`);
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
    const result = typeof table.setupSQL === 'string' && table.setupSQL.trim() && typeof wasmApi.executeMulti === 'function'
        ? await wasmApi.executeMulti(table.setupSQL)
        : await wasmApi.importFile(fileName, table.content, table.name);
    if (!result || !result.success) {
        throw new Error(result?.error || `Import failed for ${table.name}`);
    }
    const schema = typeof wasmApi.getTableSchema === 'function'
        ? await wasmApi.getTableSchema(table.name)
        : null;
    return {
        name: table.name,
        rowCount: schema?.success ? schema.rows : result.rowsImported,
        columns: schema?.success && Array.isArray(schema.columns)
            ? schema.columns.map(c => typeof c === 'object' ? String(c.name) : String(c))
            : (Array.isArray(result.columns) ? result.columns.map(c => String(c)) : []),
    };
}

async function applyHashDemoPayload(payload) {
    if (!payload || applyingHashDemo || !wasmReady || typeof wasmApi.importFile !== 'function') {
        return false;
    }
    if (!requireStableWorkspace('load shared demo data')) return false;
    openStudio({ focusEditor: false });
    applyingHashDemo = true;
    try {
        updateStatus(`Loading shared demo: ${payload.title || payload.id || 'tinySQL'}...`);
        if (typeof wasmApi.clearDatabase === 'function') {
            const cleared = await wasmApi.clearDatabase();
            if (!cleared?.success) throw new Error(cleared?.error || 'Could not clear the current workspace.');
        }
        currentTables = [];
        resetWorkspaceResultView();
        for (const key of Object.keys(pendingClientTables)) {
            delete pendingClientTables[key];
        }

        for (const table of payload.tables) {
            const tableInfo = await importDemoPayloadTable(table);
            currentTables.push(tableInfo);
            if (isRoutingGraphFile(table.fileName)) {
                await loadTables();
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
    if (decodeDemoHash()) {
        openStudio({ focusEditor: false });
    }
    setupDragDrop();
    renderHistory();
    setupEditorSyntaxHighlighting();
    restoreEditorState();
    enhanceDemoQueries();
    setupAccessibilityShortcuts();
    setupSqlAutocomplete();
    renderIntroPage();
    initWasm();

    document.addEventListener('click', (event) => {
        if (!event.target.closest('.topbar-menu')) {
            closeUtilityMenu();
        }
        if (!event.target.closest('.results-export')) {
            closeResultsExportMenu();
        }
    });
    document.addEventListener('keydown', (event) => {
        if (event.key === 'Escape') {
            closeUtilityMenu();
            closeResultsExportMenu();
            toggleRuntimePanel(false);
        }
    });
    
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
        label.includes('returning') || label.includes('view') || label.includes('analyze')) {
        return 'recent';
    }
    if (label.includes('geo') || label.includes('bbox') || label.includes('node') ||
        label.includes('route') || label.includes('distance') || label.includes('radius') ||
        label.includes('zone') || label.includes('munich') || label.includes('crs') ||
        label.includes('wms') || label.includes('tilematrix') || label.includes('ogc')) {
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
    '🧩 HYBRID_SEARCH + Wildcards': 'ai_docs',
    '🔗 RAG Context': 'rag_chunks',
    '✂️ FTS Snippet': 'ai_docs',
    '🧠 RAG_SEARCH': 'rag_chunks',
    '🔎 CONTAINS Search': 'ai_docs',
    '📐 Vector Helpers': 'ai_docs',
    '📈 ANALYZE Statistics': 'sales',
    '🎨 Choropleth Classes': 'sales',
    '🟢 GEO_BUFFER Circle': 'places_geo',
    '🧭 Route Bearing & Midpoint': 'places_geo',
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
    // Includes virtual tables (e.g. routing-graph views) and pending
    // client-side tables, not just imported/loaded ones, so demo queries
    // that target those never get stuck marked "unavailable".
    const loadedTables = new Set(getKnownTableNames().map((name) => name.toLowerCase()));
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
        return '';
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
    if (!requireStableWorkspace('import files')) return;
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
    
    let imported = 0;
    for (const file of files) {
        if (await importSingleFile(file)) {
            imported += 1;
        }
    }
    if (files.length > 1) {
        updateStatus(`Imported ${imported} of ${files.length} file(s)`);
    }
}

function readFile(file, method) {
    return new Promise((resolve, reject) => {
        const reader = new FileReader();
        reader.onload = () => resolve(reader.result);
        reader.onerror = () => reject(new Error(`Failed to read file: ${file.name}`));
        reader[method](file);
    });
}

// Import a single file. Awaiting file reads keeps multi-file imports ordered
// and avoids racing status/table updates.
async function importSingleFile(file) {
    const workspaceAtStart = activeWorkspaceId;
    const epochAtStart = workspaceEpoch;
    if (Number(file?.size) > MAX_IMPORT_BYTES) {
        const maxMiB = Math.round(MAX_IMPORT_BYTES / (1024 * 1024));
        const actualMiB = (Number(file.size) / (1024 * 1024)).toFixed(1);
        showToast(`${file.name} is ${actualMiB} MiB; browser imports are limited to ${maxMiB} MiB.`, 'error');
        updateStatus('Import rejected: file is too large');
        return false;
    }
    const fileName = file.name.toLowerCase();
    if (fileName.endsWith('.xlsx') || fileName.endsWith('.xls')) {
        return importExcelFile(file);
    }

    try {
        const content = await readFile(file, 'readAsText');
        if (workspaceAtStart !== activeWorkspaceId || epochAtStart !== workspaceEpoch || !requireStableWorkspace('import files')) {
            return false;
        }
        const tableName = sanitizeTableName(file.name);
        updateStatus(`Importing ${file.name}...`);

        if (typeof wasmApi.importFile !== 'function') {
            throw new Error('WASM importFile function not available. Make sure WASM is initialized.');
        }

        const result = await wasmApi.importFile(file.name, content, tableName);
        if (!result || typeof result !== 'object') {
            throw new Error('WASM importFile returned an invalid result');
        }
        if (!result.success) {
            throw new Error(result.error || 'Unknown import error');
        }

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
        if (isRoutingGraphFile(file.name)) {
            await loadTables();
        }

        let message = `Imported ${result.rowsImported} rows into "${tableName}"`;
        if (result.rowsSkipped > 0) {
            message += ` (${result.rowsSkipped} skipped)`;
        }
        updateStatus(message);

        const editor = document.getElementById('queryEditor');
        const defaultQuery = `SELECT * FROM ${tableName} LIMIT 10`;
        if (!editor.value || /SELECT \* FROM (mytable|table1|table2)/i.test(editor.value)) {
            editor.value = defaultQuery;
            syncEditorHighlight();
            saveEditorState();
        }

        const executeBtn = document.getElementById('executeBtn');
        if (executeBtn) executeBtn.disabled = false;
        scheduleDatabaseSnapshotSave();
        return true;
    } catch (err) {
        alert(`Import error: ${err.message}`);
        updateStatus('Import failed');
        console.error('Import failed:', err);
        return false;
    }
}

// Import Excel file using SheetJS
async function importExcelFile(file) {
    const workspaceAtStart = activeWorkspaceId;
    const epochAtStart = workspaceEpoch;
    if (typeof XLSX === 'undefined') {
        showToast('Excel support is unavailable right now. Try CSV or JSON, or refresh the page.', 'error');
        updateStatus('Excel support unavailable');
        return false;
    }

    updateStatus(`Reading Excel file: ${file.name}...`);

    try {
        const data = new Uint8Array(await readFile(file, 'readAsArrayBuffer'));
        if (workspaceAtStart !== activeWorkspaceId || epochAtStart !== workspaceEpoch || !requireStableWorkspace('import files')) {
            return false;
        }
        const workbook = XLSX.read(data, { type: 'array' });
        let importedSheets = 0;

        for (const sheetName of workbook.SheetNames) {
            if (workspaceAtStart !== activeWorkspaceId || epochAtStart !== workspaceEpoch || !requireStableWorkspace('import files')) {
                return false;
            }
            const worksheet = workbook.Sheets[sheetName];
            const jsonData = XLSX.utils.sheet_to_json(worksheet);
            if (jsonData.length === 0) {
                continue;
            }

            const tableName = sanitizeTableName(sheetName);
            updateStatus(`Importing sheet: ${sheetName}...`);
            const result = await wasmApi.importFile(`${sheetName}.json`, JSON.stringify(jsonData), tableName);
            if (!result?.success) {
                console.error(`Failed to import sheet "${sheetName}":`, result?.error || 'unknown error');
                continue;
            }

            const tableInfo = {
                name: tableName,
                rowCount: result.rowsImported,
                columns: Array.isArray(result.columns) ? result.columns.map(c => String(c)) : []
            };
            const existingIndex = currentTables.findIndex(t => t.name === tableName);
            if (existingIndex >= 0) currentTables[existingIndex] = tableInfo;
            else currentTables.push(tableInfo);
            importedSheets += 1;
        }

        renderTables();
        updateStatus(`Excel file imported: ${importedSheets} sheet(s)`);
        const executeBtn = document.getElementById('executeBtn');
        if (executeBtn) executeBtn.disabled = false;
        if (currentTables.length > 0) {
            const firstTable = currentTables[0].name;
            setQuery(`SELECT * FROM ${quoteSqlIdentifier(firstTable)} LIMIT 10`);
        }
        scheduleDatabaseSnapshotSave();
        return importedSheets > 0;
    } catch (err) {
        alert(`Failed to parse Excel file: ${err.message}`);
        updateStatus('Excel import failed');
        return false;
    }
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
        tableList.innerHTML = emptyTablesMarkup();
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
            <div class="table-item" data-table-name="${escapeHtml(table.name)}" role="button" tabindex="0" aria-label="Select table ${escapeHtml(table.name)}">
                <div class="table-name">
                    ${escapeHtml(table.name)} ${badgeHtml}
                    <span class="table-remove" data-action="remove-table" role="button" tabindex="0" title="Remove table" aria-label="Remove table ${escapeHtml(table.name)}">✕</span>
                    <span class="table-info-btn" data-action="show-table-info" role="button" tabindex="0" title="Show schema" aria-label="Show schema for ${escapeHtml(table.name)}">ℹ</span>
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
            <div class="table-section-label virtual-toggle" onclick="toggleVirtualTables()" role="button" tabindex="0">
                <span>${collapsed ? '▶' : '▼'} Virtual Tables (${virtuals.length})</span>
            </div>`;
        if (!collapsed) {
            html += virtuals.map(vt => `
                <div class="table-item virtual-table-item" data-table-name="${escapeHtml(vt.name)}" role="button" tabindex="0" aria-label="Select table ${escapeHtml(vt.name)}">
                    <div class="table-name">
                        ${escapeHtml(vt.name)}
                        <span class="table-badge virtual">virtual</span>
                        <span class="table-info-btn" data-action="show-table-info" role="button" tabindex="0" title="Show schema" aria-label="Show schema for ${escapeHtml(vt.name)}">ℹ</span>
                    </div>
                    <div class="table-meta"><span>computed at query time</span></div>
                </div>
            `).join('');
        }
    }

    tableList.innerHTML = html;
    setupTableListDelegation();
}

function emptyTablesMarkup() {
    return `
        <div class="empty-state empty-tables-state">
            <div class="empty-state-icon">📊</div>
            <div class="empty-state-title">No Tables Loaded</div>
            <div class="empty-state-text">Start with a local file or a guided sample.</div>
            <div class="empty-state-actions">
                <button class="empty-state-action" type="button" onclick="showUploadDialog()">Add a file</button>
                <button class="empty-state-action secondary" type="button" onclick="loadShareableDemo('analytics')">Try sample data</button>
            </div>
            <div class="empty-state-hint">Nothing leaves this browser.</div>
        </div>
    `;
}

// Delegated click/keydown handling for the table list. Table names can
// originate from an untrusted shared-demo URL hash, so they are never
// interpolated into inline event-handler strings (HTML-entity decoding of
// an escaped `'` would re-open the JS string and allow script injection);
// instead they travel through data-* attributes and are read back as data.
let tableListDelegationReady = false;
function setupTableListDelegation() {
    if (tableListDelegationReady) {
        return;
    }
    tableListDelegationReady = true;
    const tableList = document.getElementById('tableList');
    if (!tableList) {
        return;
    }
    tableList.addEventListener('click', (event) => {
        const actionEl = event.target.closest('[data-action]');
        if (actionEl) {
            event.stopPropagation();
            const item = actionEl.closest('[data-table-name]');
            const name = item ? item.dataset.tableName : null;
            if (!name) {
                return;
            }
            if (actionEl.dataset.action === 'remove-table') {
                removeTable(name).catch((error) => {
                    console.error('Could not remove table:', error);
                    showToast(`Could not remove table: ${error.message}`, 'error');
                });
            } else if (actionEl.dataset.action === 'show-table-info') {
                showTableInfo(name).catch((error) => {
                    console.error('Could not inspect table:', error);
                    showToast(`Could not inspect table: ${error.message}`, 'error');
                });
            }
            return;
        }
        const item = event.target.closest('[data-table-name]');
        if (item) {
            selectTable(item.dataset.tableName);
        }
    });
    tableList.addEventListener('keydown', (event) => {
        if (event.key !== 'Enter' && event.key !== ' ') {
            return;
        }
        const target = event.target.closest('[role="button"]');
        if (!target) {
            return;
        }
        event.preventDefault();
        target.click();
    });
}

// Toggle virtual table section collapsed state
function toggleVirtualTables() {
    window._virtualCollapsed = !(window._virtualCollapsed !== false);
    renderTables();
}

// Show schema / info panel for a table (real or virtual)
async function showTableInfo(tableName) {
    if (typeof wasmApi.getTableSchema !== 'function') {
        alert('Schema inspection requires WASM to be ready');
        return;
    }
    const info = await wasmApi.getTableSchema(tableName);
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
                <button type="button" class="schema-select-all">SELECT *</button>
            </div>
        `;
        const selectAllBtn = panel.querySelector('.schema-select-all');
        if (selectAllBtn) {
            // tableName is closed over directly (never re-serialized into HTML/JS
            // source), so it is safe even if it contains quotes from an untrusted
            // shared-demo payload.
            selectAllBtn.addEventListener('click', () => {
                setQuery(`SELECT * FROM ${quoteSqlIdentifier(tableName)} LIMIT 100`);
                closeSchemaPanel();
            });
        }
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
async function removeTable(tableName) {
    if (!requireStableWorkspace('remove a table')) return;
    if (!confirm(`Remove table "${tableName}"? This cannot be undone.`)) {
        return;
    }
    const isPending = Object.prototype.hasOwnProperty.call(pendingClientTables, tableName);

    if (!isPending && wasmReady && typeof wasmApi.dropTable === 'function') {
        const result = await wasmApi.dropTable(tableName);
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

async function clearAllTables() {
    if (!requireStableWorkspace('clear tables')) return;
    if (!confirm('This will remove all imported tables. Continue?')) {
        return;
    }
    try {
        if (typeof wasmApi.clearDatabase === 'function') {
            const result = await wasmApi.clearDatabase();
            if (!result || !result.success) {
                alert(`Failed to clear database: ${result?.error || 'Unknown error'}`);
                return;
            }
        }
        for (const key of Object.keys(pendingClientTables)) {
            delete pendingClientTables[key];
        }
        currentTables = [];
        resetWorkspaceResultView({ showIntro: true });
        renderTables();
        scheduleDatabaseSnapshotSave(0);
        updateStatus('Database cleared');
    } catch (error) {
        console.error('Could not clear the database:', error);
        showToast(`Could not clear the database: ${error.message || error}`, 'error');
    }
}

// Format query (basic)
function formatQuery() {
    const editor = document.getElementById('queryEditor');
    if (!editor) {
        return;
    }
    const query = editor.value.trim();

    // Only reformat whitespace/keywords/commas *outside* string literals and
    // comments. Otherwise a value like 'Doe, John' gets its comma reflowed
    // and a `-- note` comment gets mangled — and since collapsing the
    // newline that ends a line comment would silently comment out the rest
    // of the query on next execution, that newline is deliberately preserved.
    const PROTECTED_PATTERN = /(--[^\n]*|\/\*[\s\S]*?\*\/|'(?:''|[^'])*')/g;
    const KEYWORD_PATTERN = /\b(SELECT|FROM|WHERE|JOIN|LEFT JOIN|RIGHT JOIN|INNER JOIN|FULL JOIN|CROSS JOIN|ON|ORDER BY|GROUP BY|HAVING|LIMIT|OFFSET|UNION|UNION ALL|INTERSECT|EXCEPT|WITH)\b/gi;

    const formatOutside = (text, afterLineComment) => {
        const keepLeadingNewline = afterLineComment && /^\s*\n/.test(text);
        const body = text
            .replace(/\s+/g, ' ')
            .replace(KEYWORD_PATTERN, '\n$1')
            .replace(/,/g, ',\n  ');
        return keepLeadingNewline ? '\n' + body.replace(/^ /, '') : body;
    };

    let formatted = '';
    let lastIndex = 0;
    let afterLineComment = false;
    let match;
    PROTECTED_PATTERN.lastIndex = 0;
    while ((match = PROTECTED_PATTERN.exec(query)) !== null) {
        formatted += formatOutside(query.slice(lastIndex, match.index), afterLineComment);
        formatted += match[0];
        afterLineComment = match[0].startsWith('--');
        lastIndex = match.index + match[0].length;
    }
    formatted += formatOutside(query.slice(lastIndex), afterLineComment);

    editor.value = formatted.trim();
    syncEditorHighlight();
    saveEditorState();
}

// Execute query (UI handler)
async function onExecuteClick() {
    if (queryExecutionInFlight) {
        showToast('A query is already running.', 'info');
        return;
    }
    if (!requireStableWorkspace('run a query')) return;
    const editor = document.getElementById('queryEditor');
    const selectedQuery = editor.value.slice(editor.selectionStart, editor.selectionEnd).trim();
    const query = selectedQuery || editor.value.trim();
    
    if (!query) {
        alert('Please enter a query');
        return;
    }

    if (!wasmReady) {
        alert('WASM not ready yet...');
        return;
    }

    queryExecutionInFlight = true;
    activeQueryStreamProgress = null;
    if (streamPreviewRenderTimer) {
        clearTimeout(streamPreviewRenderTimer);
        streamPreviewRenderTimer = null;
    }
    const executeBtn = document.getElementById('executeBtn');
    const resultsContainer = document.getElementById('resultsContainer');
    const useStreamingQuery = !hasMultipleSQLStatements(query) &&
        typeof wasmApi.executeQueryStream === 'function';
    activeQueryAbortController = useStreamingQuery ? new AbortController() : null;

    executeBtn.disabled = true;
    executeBtn.innerHTML = '<span class="spinner"></span> Running…';
    setQueryCancellationState(useStreamingQuery);
    if (resultsContainer) {
        resultsContainer.setAttribute('aria-busy', 'true');
    }
    setOpenVanillaGridEnabled(false);
    
    updateStatus('Executing query…');
    setRuntimeActivityHint(1);
    await new Promise((resolve) => requestAnimationFrame(() => requestAnimationFrame(resolve)));

    try {
        if (!useStreamingQuery && typeof wasmApi.executeQuery !== 'function') {
            throw new Error('WASM executeQuery function not available');
        }

        const startTime = performance.now();
        // A single statement (including one with a trailing semicolon) uses
        // the bounded ResultStream preview. Scripts keep the established
        // materialized semantics until multi-statement streaming is added.
        const hasMulti = hasMultipleSQLStatements(query) && typeof wasmApi.executeMulti === 'function';
        const result = useStreamingQuery
            ? await wasmApi.executeQueryStream(query, { signal: activeQueryAbortController.signal })
            : hasMulti
                ? await wasmApi.executeMulti(query)
                : await wasmApi.executeQuery(query);
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
                page: 1,
                pageSize: DEFAULT_RESULT_PAGE_SIZE,
            };
            currentResults = {
                columns: cols,
                rows: rows,
                rowCount: Number.isFinite(Number(result.totalRows)) ? Number(result.totalRows) : rows.length,
                filteredRowCount: Number.isFinite(Number(result.filteredRows)) ? Number(result.filteredRows) : rows.length,
                pageOffset: Number.isFinite(Number(result.pageOffset)) ? Number(result.pageOffset) : 0,
                serverPaged: Boolean(result.serverPaged && typeof wasmApi.getResultPage === 'function'),
                pageKey: `1|${DEFAULT_RESULT_PAGE_SIZE}|||asc`,
                duration: duration,
                streamed: Boolean(result.streamed),
                previewOnly: Boolean(result.previewOnly),
                truncated: Boolean(result.truncated),
                truncationReason: result.truncationReason ? String(result.truncationReason) : '',
                resultLimitRows: Number(result.resultLimitRows) || 0,
                resultLimitBytes: Number(result.resultLimitBytes) || 0,
                rowsScanned: Number(result.rowsScanned) || 0,
                rowsProduced: Number(result.rowsProduced) || 0,
                resultBytes: Number(result.resultBytes) || 0,
                materialized: Boolean(result.materialized),
            };
            await renderResults(currentResults);
            pushHistory(query, duration, currentResults.rowCount);
            saveEditorState();
            if (sqlMayMutate(query)) {
                await loadTables();
                scheduleDatabaseSnapshotSave();
            }
            const resultCount = currentResults.truncated
                ? `${currentResults.rowCount.toLocaleString()}+ preview rows`
                : `${currentResults.rowCount.toLocaleString()} rows`;
            updateStatus(`Query completed: ${resultCount} in ${duration}${result.statementsRun > 1 ? ` (${result.statementsRun} statements)` : ''}`);
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
        const cancelled = error?.name === 'AbortError';
        currentResults = null;
        resultsContainer.innerHTML = cancelled ? `
            <div class="empty-state">
                <div class="empty-state-icon">■</div>
                <div class="empty-state-title">Query cancelled</div>
                <div class="empty-state-text">No partial result was retained. Refine the query or run it again.</div>
            </div>
        ` : `
            <div class="error-message">
                <strong>Error:</strong> ${escapeHtml(error.message)}
            </div>
        `;
        window.clearVanillaGrid?.();
        setOpenVanillaGridEnabled(false);
        updateStatus(cancelled ? 'Query cancelled' : 'Query failed');
    } finally {
        queryExecutionInFlight = false;
        activeQueryAbortController = null;
        activeQueryStreamProgress = null;
        if (streamPreviewRenderTimer) {
            clearTimeout(streamPreviewRenderTimer);
            streamPreviewRenderTimer = null;
        }
        executeBtn.disabled = false;
        executeBtn.innerHTML = '▶ Execute';
        setQueryCancellationState(false);
        if (resultsContainer) {
            resultsContainer.setAttribute('aria-busy', 'false');
        }
        await refreshRuntimeStatus();
    }
}

// Render query results
async function renderResults(data) {
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

    const pageSize = RESULT_PAGE_SIZES.includes(resultViewState.pageSize)
        ? resultViewState.pageSize
        : DEFAULT_RESULT_PAGE_SIZE;
    const pageKey = `${resultViewState.page}|${pageSize}|${resultViewState.filterText}|${resultViewState.sortColumn}|${resultViewState.sortDirection}`;
    if (data.serverPaged && data.pageKey !== pageKey) {
        const requestedPageKey = pageKey;
        const pageOffset = (Math.max(1, resultViewState.page) - 1) * pageSize;
        let pageResult;
        try {
            pageResult = await wasmApi.getResultPage(
                pageOffset,
                pageSize,
                resultViewState.filterText,
                resultViewState.sortColumn,
                resultViewState.sortDirection
            );
        } catch (error) {
            if (data !== currentResults || isWorkspaceTransitioning()) return;
            resultsContainer.innerHTML = `
                <div class="error-message">
                    <strong>Result paging failed:</strong> ${escapeHtml(error.message || error)}
                </div>
            `;
            return;
        }
        const latestPageKey = `${resultViewState.page}|${resultViewState.pageSize}|${resultViewState.filterText}|${resultViewState.sortColumn}|${resultViewState.sortDirection}`;
        if (data !== currentResults || latestPageKey !== requestedPageKey) {
            return;
        }
        if (!pageResult || !pageResult.success) {
            resultsContainer.innerHTML = `
                <div class="error-message">
                    <strong>Result paging failed:</strong> ${escapeHtml(pageResult?.error || 'Unknown error')}
                </div>
            `;
            return;
        }
        data.rows = Array.isArray(pageResult.rows) ? pageResult.rows : [];
        data.filteredRowCount = Number(pageResult.filteredRows) || 0;
        data.pageOffset = Number(pageResult.pageOffset) || 0;
        data.pageKey = pageKey;
    }

    const visible = getVisibleResults(data);
    const displayedRows = visible ? visible.rows : [];
    const displayedColumns = visible ? visible.columns : [];
    const totalRows = data.serverPaged ? data.rowCount : data.rows.length;
    const filteredRows = data.serverPaged ? data.filteredRowCount : displayedRows.length;
    const totalPages = Math.max(1, Math.ceil(filteredRows / pageSize));
    const page = Math.min(Math.max(1, resultViewState.page), totalPages);
    resultViewState.page = page;
    const rowStart = data.serverPaged ? data.pageOffset : (page - 1) * pageSize;
    const renderedRows = data.serverPaged ? displayedRows : displayedRows.slice(rowStart, rowStart + pageSize);
    const renderIsPaginated = filteredRows > pageSize;
    const rowEnd = rowStart + renderedRows.length;
    const previewProgress = data.materialized ? 'rows materialized' : 'rows scanned';
    const previewNotice = data.previewOnly
        ? `<br><span class="result-preview-notice">${data.truncated
            ? `Bounded preview: stopped at the ${escapeHtml(data.truncationReason || 'result')} cap (${Number(data.rowsScanned || 0).toLocaleString()} ${previewProgress}). Add LIMIT or refine the query for a complete export.`
            : `Streamed preview: ${Number(data.rowsScanned || 0).toLocaleString()} ${previewProgress}. Full-result export is intentionally unavailable in this mode.`
        }</span>`
        : '';
    const exportActions = data.previewOnly
        ? '<span class="result-preview-badge" title="Use a LIMIT or refine the query before exporting a complete result">Preview only</span>'
        : `<div class="results-export">
                <button id="resultsExportTrigger" onclick="toggleResultsExportMenu()" aria-expanded="false" aria-controls="resultsExportMenu">Download</button>
                <div id="resultsExportMenu" class="results-export-menu hidden" role="menu" aria-label="Download result as a file">
                    <button role="menuitem" onclick="doExport('csv'); closeResultsExportMenu()">CSV</button>
                    <button role="menuitem" onclick="doExport('tsv'); closeResultsExportMenu()">TSV</button>
                    <button role="menuitem" onclick="doExport('xlsx'); closeResultsExportMenu()">Excel (.xlsx, page)</button>
                    <button role="menuitem" onclick="doExport('json'); closeResultsExportMenu()">JSON</button>
                    <button role="menuitem" onclick="doExport('xml'); closeResultsExportMenu()">XML</button>
                    <button role="menuitem" onclick="doExport('html'); closeResultsExportMenu()">HTML table (page)</button>
                    <button role="menuitem" onclick="doExport('md'); closeResultsExportMenu()">Markdown</button>
                </div>
            </div>`;

    if (!visible || filteredRows === 0) {
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
                <strong>${filteredRows}</strong> / <strong>${totalRows}</strong> rows •
                <strong>${displayedColumns.length}</strong> columns •
                ${data.duration}
                ${renderIsPaginated ? `<br><span>Showing rows ${rowStart + 1}–${rowEnd}. Filtering and sorting run in WASM; copy and grid use this page.</span>` : ''}
                ${previewNotice}
            </div>
            <div class="results-actions">
                <button onclick="copyResultsToClipboard()">Copy Results</button>
                <button id="openVanillaGridBtn" onclick="openInVanillaGrid()" disabled title="Pivot the current result page and use the included D3 chart controls">Pivot &amp; Charts</button>
                <div class="results-export">
                    <button id="codeGeneratorTrigger" onclick="toggleCodeGeneratorMenu()" aria-expanded="false" aria-controls="codeGeneratorMenu">Generate Code</button>
                    <div id="codeGeneratorMenu" class="results-export-menu hidden" role="menu" aria-label="Generate code from result">
                        <button role="menuitem" onclick="showGeneratedCode('go')">Go struct</button>
                        <button role="menuitem" onclick="showGeneratedCode('typescript')">TypeScript interface</button>
                        <button role="menuitem" onclick="showGeneratedCode('python')">Python dataclass</button>
                        <button role="menuitem" onclick="showGeneratedCode('sql')">SQL CREATE TABLE</button>
                    </div>
                </div>
                ${exportActions}
            </div>
        </div>
        <div class="results-toolbar">
            <label>
                Filter
                <input id="resultFilterInput" type="search" value="${escapeHtml(resultViewState.filterText)}" placeholder="Search rows..." oninput="scheduleResultFilterUpdate()">
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
            <label>
                Rows per page
                <select id="resultPageSize" onchange="updateResultPageSize()">
                    ${RESULT_PAGE_SIZES.map((size) => `<option value="${size}" ${pageSize === size ? 'selected' : ''}>${size}</option>`).join('')}
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
                            <button class="column-sort-button" data-col="${escapeHtml(col)}" title="Sort by ${escapeHtml(col)}">
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
                        <td class="row-num-col">${rowStart + idx + 1}</td>
                        ${displayedColumns.map(col => {
                            const value = row[col];
                            return formatCell(value);
                        }).join('')}
                    </tr>
                `).join('')}
            </tbody>
        </table>
        </div>
        ${renderIsPaginated ? `
            <div class="result-pagination" aria-label="Result pages">
                <button onclick="changeResultPage(-1)" ${page === 1 ? 'disabled' : ''}>Previous</button>
                <span>Page ${page} of ${totalPages}</span>
                <button onclick="changeResultPage(1)" ${page === totalPages ? 'disabled' : ''}>Next</button>
            </div>
        ` : ''}
    `;

    resultsContainer.innerHTML = tableHtml;
    setOpenVanillaGridEnabled(true);
    setupResultsSortDelegation(resultsContainer);
}

// Delegated click handling for column-sort-button headers. Column names can
// come straight from imported file headers or an untrusted shared-demo
// payload, so they travel via the data-col attribute instead of being
// interpolated into an inline onclick string.
let resultsSortDelegationReady = false;
function setupResultsSortDelegation(resultsContainer) {
    if (resultsSortDelegationReady) {
        return;
    }
    resultsSortDelegationReady = true;
    resultsContainer.addEventListener('click', (event) => {
        const btn = event.target.closest('.column-sort-button');
        if (btn) {
            sortResultsBy(btn.dataset.col);
        }
    });
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
    if (!requireStableWorkspace('import files')) return;
    document.getElementById('fileInput').click();
}

// Load tables (for refresh button)
async function loadTables() {
    const workspaceAtStart = activeWorkspaceId;
    const epochAtStart = workspaceEpoch;
    if (typeof wasmApi.listTables === 'function') {
        try {
            const info = await wasmApi.listTables();
            if (workspaceAtStart !== activeWorkspaceId || epochAtStart !== workspaceEpoch) return;
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
        container.setAttribute('role', 'status');
        container.setAttribute('aria-live', 'polite');
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

window.showToast = showToast;

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
    if (typeof window.renderVanillaGrid !== 'function') {
        showToast('Pivot Grid is unavailable. The table view and exports still work.', 'error');
        return;
    }
    window.renderVanillaGrid?.(visible);
}

function getVisibleResults(source = currentResults) {
    if (!source || !Array.isArray(source.rows) || !Array.isArray(source.columns)) {
        return null;
    }

    if (source.serverPaged) {
        return {
            columns: source.columns.slice(),
            rows: source.rows.slice(),
            rowCount: source.filteredRowCount,
            duration: source.duration,
        };
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

let resultFilterTimer = null;
function scheduleResultFilterUpdate() {
    // Debounced so filtering the (potentially 5-10k row) results table
    // doesn't rebuild the whole grid on every keystroke.
    window.clearTimeout(resultFilterTimer);
    resultFilterTimer = window.setTimeout(() => {
        updateResultViewState().catch((error) => {
            console.error('Could not update the result view:', error);
            updateStatus(`Could not update the result view: ${error.message || error}`);
        });
    }, 200);
}

async function updateResultViewState() {
    const filterInput = document.getElementById('resultFilterInput');
    const sortSelect = document.getElementById('resultSortColumn');
    const sortDirection = document.getElementById('resultSortDirection');
    const keepFilterFocus = document.activeElement && document.activeElement.id === 'resultFilterInput';
    const selectionStart = keepFilterFocus && typeof filterInput?.selectionStart === 'number' ? filterInput.selectionStart : null;
    const selectionEnd = keepFilterFocus && typeof filterInput?.selectionEnd === 'number' ? filterInput.selectionEnd : null;

    const filterText = filterInput ? filterInput.value : resultViewState.filterText;
    const sortColumn = sortSelect ? sortSelect.value : resultViewState.sortColumn;
    const sortDirectionValue = sortDirection ? sortDirection.value : resultViewState.sortDirection;
    if (filterText !== resultViewState.filterText ||
        sortColumn !== resultViewState.sortColumn ||
        sortDirectionValue !== resultViewState.sortDirection) {
        resultViewState.page = 1;
    }
    resultViewState.filterText = filterText;
    resultViewState.sortColumn = sortColumn;
    resultViewState.sortDirection = sortDirectionValue;

    if (currentResults) {
        await renderResults(currentResults);
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

async function clearResultViewFilters() {
    resultViewState.filterText = '';
    resultViewState.sortColumn = '';
    resultViewState.sortDirection = 'asc';
    resultViewState.page = 1;
    if (currentResults) {
        await renderResults(currentResults);
    }
}

async function sortResultsBy(column) {
    if (!currentResults || !Array.isArray(currentResults.columns) || !currentResults.columns.includes(column)) {
        return;
    }

    if (resultViewState.sortColumn === column) {
        resultViewState.sortDirection = resultViewState.sortDirection === 'asc' ? 'desc' : 'asc';
    } else {
        resultViewState.sortColumn = column;
        resultViewState.sortDirection = 'asc';
    }

    resultViewState.page = 1;

    await renderResults(currentResults);
}

async function updateResultPageSize() {
    const pageSize = Number(document.getElementById('resultPageSize')?.value);
    if (!RESULT_PAGE_SIZES.includes(pageSize)) {
        return;
    }
    resultViewState.pageSize = pageSize;
    resultViewState.page = 1;
    if (currentResults) {
        await renderResults(currentResults);
    }
}

async function changeResultPage(delta) {
    if (!currentResults || !Number.isInteger(delta)) {
        return;
    }
    const visible = getVisibleResults(currentResults);
    const resultCount = currentResults.serverPaged ? currentResults.filteredRowCount : visible.rows.length;
    const totalPages = Math.max(1, Math.ceil(resultCount / resultViewState.pageSize));
    resultViewState.page = Math.min(Math.max(1, resultViewState.page + delta), totalPages);
    await renderResults(currentResults);
}

function getSortIndicator(column) {
    if (resultViewState.sortColumn !== column) {
        return '';
    }
    return resultViewState.sortDirection === 'asc' ? '▲' : '▼';
}

// Unified export dispatcher – tries WASM-side first, falls back to client-side
async function doExport(format) {
    if (currentResults?.previewOnly) {
        alert('This result is a bounded stream preview. Add a LIMIT or refine the query before exporting a complete result.');
        return;
    }
    const visible = getVisibleResults();
    if (!visible || !visible.rows || visible.rows.length === 0) {
        alert('No results to export');
        return;
    }
    const viewIsRaw = !resultViewState.filterText.trim() && !resultViewState.sortColumn;
    // XLSX and HTML are deliberately assembled in the UI: XLSX is binary and
    // the HTML export follows the currently visible column order. The text
    // formats can stay in WASM for an unfiltered complete result.
    const clientOnlyFormat = format === 'xlsx' || format === 'html';
    if (!clientOnlyFormat && viewIsRaw && typeof wasmApi.exportResults === 'function') {
        try {
            const res = await wasmApi.exportResults(format);
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
    else if (format === 'xlsx') exportXLSX(visible);
    else if (format === 'md') exportMarkdown(visible);
    else if (format === 'json') exportJSON(visible);
    else if (format === 'xml') exportXML(visible);
    else if (format === 'html') exportHTML(visible);
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

// Export an actual workbook instead of a CSV file with a misleading .xlsx
// suffix. Spreadsheet formulas from imported data are stored as text so a
// download cannot unexpectedly execute them when opened.
function exportXLSX(visible = getVisibleResults()) {
    if (!visible || !visible.rows || visible.rows.length === 0) {
        alert('No results to export');
        return;
    }
    if (!window.XLSX?.utils) {
        showToast('Excel export is unavailable because the spreadsheet library did not load.', 'error');
        return;
    }

    const safeCell = (value) => {
        if (value === null || value === undefined) return '';
        if (typeof value === 'number' || typeof value === 'boolean') return value;
        const text = String(value);
        return /^[=+\-@]/.test(text) ? `'${text}` : text;
    };
    const sheetRows = [visible.columns.slice()];
    for (const row of visible.rows) {
        sheetRows.push(visible.columns.map((column) => safeCell(row[column])));
    }
    const sheet = window.XLSX.utils.aoa_to_sheet(sheetRows);
    sheet['!cols'] = visible.columns.map((column, index) => {
        const values = sheetRows.slice(0, 1000).map((row) => String(row[index] ?? ''));
        return { wch: Math.min(48, Math.max(10, String(column).length + 2, ...values.map((value) => value.length + 2))) };
    });
    const workbook = window.XLSX.utils.book_new();
    window.XLSX.utils.book_append_sheet(workbook, sheet, 'Query results');
    const bytes = window.XLSX.write(workbook, { bookType: 'xlsx', type: 'array' });
    downloadFile(bytes, 'query_results.xlsx', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
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

function exportHTML(visible = getVisibleResults()) {
    if (!visible || !visible.rows || visible.rows.length === 0) {
        alert('No results to export');
        return;
    }
    const header = visible.columns.map((column) => `<th>${escapeHtml(column)}</th>`).join('');
    const body = visible.rows.map((row) => `<tr>${visible.columns.map((column) => (
        `<td>${escapeHtml(row[column] == null ? '' : String(row[column]))}</td>`
    )).join('')}</tr>`).join('\n');
    const html = `<!doctype html>
<html lang="en"><head><meta charset="utf-8"><title>tinySQL query results</title>
<style>body{font-family:system-ui,sans-serif;margin:2rem}table{border-collapse:collapse;width:100%}th,td{border:1px solid #bbb;padding:.45rem;text-align:left;vertical-align:top}th{background:#f3f4f6}</style>
</head><body><h1>Query results</h1><table><thead><tr>${header}</tr></thead><tbody>${body}</tbody></table></body></html>`;
    downloadFile(html, 'query_results.html', 'text/html;charset=utf-8');
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
    if (!requireStableWorkspace('import clipboard data')) return;
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

    if (!requireStableWorkspace('import clipboard data')) return;
    if (!wasmReady || typeof wasmApi.importFile !== 'function') {
        alert('WASM import is not ready yet');
        return;
    }

    try {
        updateStatus(`Importing clipboard data into ${tableName}...`);
        const result = await wasmApi.importFile(`${tableName}${ext}`, text, tableName);
        if (result && result.success) {
            await loadTables();
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
    } catch (error) {
        console.error('Clipboard import failed:', error);
        updateStatus('Clipboard import failed');
        showToast(`Clipboard import failed: ${error.message || error}`, 'error');
    }
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
        const AUTO_CLOSERS = new Set([')', "'", '"']);
        if (AUTO_PAIRS[event.key] && !event.ctrlKey && !event.metaKey && !event.altKey) {
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
            const nextChar = event.target.value.charAt(end);
            if (nextChar === AUTO_PAIRS[event.key] && event.key !== '(') {
                // Typing a quote right before its own auto-inserted closer: step over it
                // instead of inserting a second one.
                event.preventDefault();
                event.target.selectionStart = event.target.selectionEnd = end + 1;
                syncEditorHighlight();
                return;
            }
            event.preventDefault();
            const pair = event.key + AUTO_PAIRS[event.key];
            event.target.value = event.target.value.substring(0, start) + pair + event.target.value.substring(end);
            event.target.selectionStart = event.target.selectionEnd = start + 1;
            syncEditorHighlight();
            return;
        }
        if (AUTO_CLOSERS.has(event.key) && !event.ctrlKey && !event.metaKey && !event.altKey) {
            const start = event.target.selectionStart;
            const end = event.target.selectionEnd;
            if (start === end && event.target.value.charAt(start) === event.key) {
                // Step over an auto-inserted closing bracket/quote instead of
                // inserting a redundant one.
                event.preventDefault();
                event.target.selectionStart = event.target.selectionEnd = start + 1;
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
