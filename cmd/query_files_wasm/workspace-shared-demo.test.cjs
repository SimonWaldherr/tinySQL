const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');

const source = fs.readFileSync(`${__dirname}/app.js`, 'utf8');
function section(start, end) {
    const a = source.indexOf(start), b = source.indexOf(end, a);
    assert.ok(a >= 0 && b > a, `missing ${start}`);
    return source.slice(a, b);
}

function fixture({storage = true, failure = '', blocked = false, pending = false} = {}) {
    const saved = [], events = [], timers = new Map();
    let timerId = 0, releaseClear;
    const control = {database: 'original-data', failure, waitClear: false};
    const sandbox = {
        control, saved, events,
        window: {
            setTimeout(fn) { const id = ++timerId; timers.set(id, fn); return id; },
            clearTimeout(id) { timers.delete(id); },
        },
        console: {error() {}, warn() {}}, showToast: msg => events.push(msg),
        setWorkspaceControlsBusy() {}, hasActiveWorkspaceDataOperation: () => false,
        openStudio() {}, updateStatus() {}, stopLiveJobWatch() {}, startLiveJobWatch() {},
        resetWorkspaceResultView() {}, renderTables() {}, showDemoQueries() {}, setQuery() {},
        isRoutingGraphFile: () => false, loadTables: async () => {},
        refreshWorkspaceSelector: async () => {}, storageSet() {}, onExecuteClick: async () => {},
        wasmApi: {
            importFile() {},
            async clearDatabase() {
                control.database = 'empty-demo';
                if (control.waitClear) await new Promise(resolve => { releaseClear = resolve; });
                return {success: true};
            },
        },
        importDemoPayloadTable: async table => {
            if (control.failure === 'import') throw new Error('import failed');
            control.database = `demo:${table.name}`;
            return {name: table.name};
        },
        store: storage ? {async createWorkspace() {
            if (control.failure === 'create') throw new Error('create failed');
            return {id: 'demo-id', name: 'Demo: Sample'};
        }} : null,
    };
    vm.runInNewContext(`
        let workspaceTransition = null, queryExecutionInFlight = false;
        let workspaceRestoreBlocked = ${blocked}, wasmReady = true, applyingHashDemo = false;
        let workspaceStore = store, activeWorkspaceId = 'original-id', activeWorkspaceName = 'Original';
        let workspaceEpoch = 0, snapshotDirty = false, snapshotRevision = 0;
        let snapshotSaveTimer = null, snapshotIdleHandle = null;
        let currentTables = [], pendingClientTables = ${pending ? "{existing: []}" : '{}'};
        const ACTIVE_WORKSPACE_KEY = 'active';
        function isWorkspaceTransitioning() { return workspaceTransition !== null; }
        async function saveDatabaseSnapshotNow() {
            if (control.failure === 'save') return false;
            saved.push([activeWorkspaceId, control.database]);
            snapshotDirty = false;
            return true;
        }
        ${section('function runWorkspaceTransition(', 'function resetWorkspaceResultView(')}
        ${section('function scheduleDatabaseSnapshotSave(', 'async function restoreDatabaseSnapshot(')}
        ${section('async function applyHashDemoPayload(', 'async function applyHashDemoFromLocation(')}
        globalThis.apply = applyHashDemoPayload;
        globalThis.schedule = scheduleDatabaseSnapshotSave;
        globalThis.active = () => activeWorkspaceId;
    `, sandbox);
    const fireTimer = () => {
        const first = timers.entries().next();
        assert.equal(first.done, false, 'expected autosave timer');
        const [id, callback] = first.value;
        timers.delete(id); callback();
    };
    return {sandbox, saved, events, control, fireTimer, release: () => releaseClear()};
}

const payload = {title: 'Sample', tables: [{name: 'demo'}], query: 'SELECT 1'};
const flush = async () => { for (let i = 0; i < 12; i++) await Promise.resolve(); };

test('hash change saves the original and keeps timers out of the demo transition', async () => {
    const f = fixture();
    f.sandbox.schedule(0);
    f.control.waitClear = true;
    const loading = f.sandbox.apply(payload);
    await flush();
    assert.equal(f.sandbox.active(), 'demo-id');
    assert.equal(f.control.database, 'empty-demo');
    f.fireTimer();
    assert.deepEqual(f.saved.map(pair => Array.from(pair)), [['original-id', 'original-data']]);
    f.release();
    assert.equal(await loading, true);
    f.fireTimer();
    assert.deepEqual(f.saved.map(pair => Array.from(pair)), [['original-id', 'original-data'], ['demo-id', 'demo:demo']]);
});

test('save and import failures never write a demo into the original workspace', async () => {
    const cannotSave = fixture({failure: 'save'});
    assert.equal(await cannotSave.sandbox.apply(payload), false);
    assert.equal(cannotSave.sandbox.active(), 'original-id');
    assert.equal(cannotSave.control.database, 'original-data');
    const cannotImport = fixture({failure: 'import'});
    assert.equal(await cannotImport.sandbox.apply(payload), false);
    assert.deepEqual(cannotImport.saved.map(pair => Array.from(pair)), [['original-id', 'original-data']]);
    assert.equal(cannotImport.sandbox.active(), 'demo-id');
    assert.equal(cannotImport.control.database, 'empty-demo');
});

test('storage-free demos run only in a fresh blocked startup, never over active data', async () => {
    const active = fixture({storage: false});
    assert.equal(await active.sandbox.apply(payload), false);
    assert.equal(active.control.database, 'original-data');
    const fresh = fixture({storage: false, blocked: true});
    assert.equal(await fresh.sandbox.apply(payload, {freshTemporary: true}), true);
    assert.equal(fresh.control.database, 'demo:demo');
    assert.deepEqual(fresh.saved, []);
    const queued = fixture({storage: false, blocked: true, pending: true});
    assert.equal(await queued.sandbox.apply(payload, {freshTemporary: true}), false);
    assert.equal(queued.control.database, 'original-data');
});
