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

function fixture() {
    const button = { disabled: true };
    const status = [];
    const calls = { exports: 0, saves: 0, imports: 0, transitions: 0 };
    const control = { mode: 'failure', pending: null, payload: null };
    const demoCalls = [];
    const sandbox = {
        button, status, calls, control, demoCalls,
        window: {location: {hash: ''}},
        document: {
            querySelector: () => ({classList: {add() {}, remove() {}}}),
            getElementById: id => id === 'executeBtn' ? button : null,
        },
        console: { log() {}, warn() {}, error() {} },
        initializeWorkspaceStorage: async () => true,
        updateStatus: text => status.push(text),
        showToast() {}, setWorkspaceControlsBusy() {}, hasActiveWorkspaceDataOperation: () => false,
        refreshRuntimeStatus: async () => {}, scheduleRuntimeStatusRefresh() {},
        getRuntimeSessionId: () => 'test', decodeDemoHash: () => control.payload,
        applyHashDemoPayload: async (_payload, options) => {
            demoCalls.push({imports: calls.imports, blocked: sandbox.state().workspaceRestoreBlocked, freshTemporary: Boolean(options?.freshTemporary)});
            return true;
        },
        completeLegacySnapshotMigration() {}, loadTables: async () => {},
        validateWorkspaceSnapshot: async () => ({success: true}),
        importWorkspaceSnapshot: async () => { calls.imports++; return {success: control.mode !== 'import-failure'}; },
        exportWorkspaceSnapshot: async () => { calls.exports++; return {snapshot: 'empty', format: 'raw'}; },
        refreshWorkspaceSelector: async () => {},
        TinySQLWasmClient: class {
            on() {}
            async init() {}
            supports() { return false; }
        },
        workspaceStoreFixture: {
            async recoverWorkspace(_id, options) {
                if (control.pending) await control.pending;
                if (control.mode === 'failure') throw new Error('storage unavailable');
                if (control.mode === 'empty') return {snapshot: null, recovered: false};
                assert.equal(await options.validate('snapshot'), true);
                return {snapshot: 'snapshot', recovered: false};
            },
            async saveWorkspace() { calls.saves++; return {}; },
        },
    };
    vm.runInNewContext(`
        let wasmReady = false, workspaceRestoreBlocked = false, workspaceTransition = null;
        let queryExecutionInFlight = false, wasmEngine = null;
        let wasmApi = {importDatabaseBytes: async () => ({}), exportDatabaseBytes: async () => ({})};
        let workspaceStore = workspaceStoreFixture, activeWorkspaceId = 'existing', activeWorkspaceName = 'Existing';
        let currentTables = [], pendingClientTables = {}, snapshotRevision = 0, snapshotDirty = false;
        let activeWorkerRequests = new Map(), lastAppliedHash = '';
        function isWorkspaceTransitioning() { return workspaceTransition !== null; }
        function scheduleDatabaseSnapshotSave() {}
        ${section('function runWorkspaceTransition(', 'function resetWorkspaceResultView(')}
        ${section('async function saveDatabaseSnapshotNow(', 'function scheduleDatabaseSnapshotSave(')}
        ${section('async function restoreDatabaseSnapshot(', 'async function refreshWorkspaceSelector(')}
        ${section('async function initWasm(', '// Demo data')}
        globalThis.init = initWasm;
        globalThis.save = saveDatabaseSnapshotNow;
        globalThis.requireStable = requireStableWorkspace;
        globalThis.transition = () => runWorkspaceTransition(() => { calls.transitions++; return true; });
        globalThis.state = () => ({wasmReady, workspaceRestoreBlocked});
        globalThis.clearStore = () => { workspaceStore = null; };
    `, sandbox);
    return sandbox;
}

test('a failed startup recovery blocks saves and workspace transitions until a successful retry', async () => {
    const f = fixture();
    await f.init();
    assert.equal(f.state().wasmReady, false);
    assert.equal(f.state().workspaceRestoreBlocked, true);
    assert.equal(f.button.disabled, true);
    assert.match(f.status.at(-1), /Saved workspace could not be restored/);
    assert.equal(await f.save(), false);
    assert.equal(await f.transition(), false);
    assert.equal(f.requireStable('run a query'), false);
    assert.deepEqual(f.calls, {exports: 0, saves: 0, imports: 0, transitions: 0});
    f.control.mode = 'valid';
    await f.init();
    assert.equal(f.state().workspaceRestoreBlocked, false);
    assert.equal(f.state().wasmReady, true);
    assert.equal(f.button.disabled, false);
    assert.equal(f.calls.imports, 1);
});

test('empty workspaces start normally and a failed final import stays blocked', async () => {
    const empty = fixture();
    empty.control.mode = 'empty';
    await empty.init();
    assert.equal(empty.state().wasmReady, true);
    assert.equal(empty.state().workspaceRestoreBlocked, false);
    assert.equal(empty.button.disabled, false);

    const broken = fixture();
    broken.control.mode = 'import-failure';
    await broken.init();
    assert.equal(broken.state().workspaceRestoreBlocked, true);
    assert.equal(broken.button.disabled, true);
    assert.equal(await broken.save(), false);
});

test('workspace writes stay blocked while restoration is pending', async () => {
    const f = fixture();
    f.control.mode = 'valid';
    let release;
    f.control.pending = new Promise(resolve => { release = resolve; });
    const initialization = f.init();
    for (let i = 0; i < 8; i++) await Promise.resolve();
    assert.equal(f.state().workspaceRestoreBlocked, true);
    assert.equal(f.button.disabled, true);
    assert.equal(await f.transition(), false);
    assert.equal(await f.save(), false);
    release();
    await initialization;
    assert.equal(f.state().workspaceRestoreBlocked, false);
    assert.equal(f.button.disabled, false);
});

test('startup restores a saved workspace before a shared demo, and stops if recovery fails', async () => {
    const f = fixture();
    f.control.mode = 'valid';
    f.control.payload = {id: 'demo', autoRun: false};
    f.window.location.hash = '#demo';
    await f.init();
    assert.deepEqual(f.demoCalls, [{imports: 1, blocked: false, freshTemporary: false}]);
    const broken = fixture();
    broken.control.payload = {id: 'demo', autoRun: false};
    broken.window.location.hash = '#demo';
    await broken.init();
    assert.deepEqual(broken.demoCalls, []);
    assert.equal(broken.button.disabled, true);
});

test('an initial shared demo can run in a fresh temporary engine without storage', async () => {
    const f = fixture();
    f.clearStore();
    f.control.payload = {id: 'demo', autoRun: false};
    f.window.location.hash = '#demo';
    await f.init();
    assert.deepEqual(f.demoCalls, [{imports: 0, blocked: true, freshTemporary: true}]);
    assert.equal(f.state().workspaceRestoreBlocked, false);
    assert.equal(f.button.disabled, false);
});
