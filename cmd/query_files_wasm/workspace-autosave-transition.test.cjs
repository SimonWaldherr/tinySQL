const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');

// Use the real transition and timer code with deterministic timer and worker
// scheduling so an autosave can become due while a new database is importing.
function fixture(useIdleCallback) {
    const source = fs.readFileSync(`${__dirname}/app.js`, 'utf8');
    const slice = (from, to) => {
        const start = source.indexOf(from), end = source.indexOf(to, start);
        assert.ok(start >= 0 && end > start, `missing ${from}`);
        return source.slice(start, end);
    };
    let next = 1;
    const timers = new Map(), idle = new Map(), saves = [];
    const window = {
        setTimeout(fn) { const id = next++; timers.set(id, fn); return id; },
        clearTimeout(id) { timers.delete(id); },
    };
    if (useIdleCallback) {
        window.requestIdleCallback = fn => { const id = next++; idle.set(id, fn); return id; };
        window.cancelIdleCallback = id => idle.delete(id);
    }
    const sandbox = { window, console, saves, activeId: 'old', workerDatabase: 'old-db',
        showToast() {}, setWorkspaceControlsBusy() {}, hasActiveWorkspaceDataOperation: () => false,
        saveDatabaseSnapshotNow: async () => { saves.push([sandbox.activeId, sandbox.workerDatabase]); },
    };
    const program = `
        let workspaceTransition = null, queryExecutionInFlight = false, workspaceRestoreBlocked = false;
        let snapshotSaveTimer = null, snapshotIdleHandle = null, snapshotDirty = false, snapshotRevision = 0;
        function isWorkspaceTransitioning() { return workspaceTransition !== null; }
        ${slice('function runWorkspaceTransition(', 'function requireStableWorkspace(')}
        ${slice('function scheduleDatabaseSnapshotSave(', 'async function restoreDatabaseSnapshot(')}
        globalThis.start = runWorkspaceTransition;
        globalThis.schedule = scheduleDatabaseSnapshotSave;
        globalThis.setDirty = value => { snapshotDirty = value; };
    `;
    vm.runInNewContext(program, sandbox);
    const fire = queue => {
        const first = queue.entries().next();
        assert.equal(first.done, false, 'expected scheduled callback');
        const [id, callback] = first.value;
        queue.delete(id);
        callback();
    };
    return { sandbox, timers, idle, saves, fire };
}

for (const idleCallback of [false, true]) {
    test(`autosave waits for a workspace transition (idle callback ${idleCallback})`, async () => {
        const f = fixture(idleCallback);
        f.sandbox.schedule(0);
        // In the idle case the timeout already fired, but the idle callback
        // has not yet run when the transition begins.
        if (idleCallback) f.fire(f.timers);
        let release;
        const gate = new Promise(resolve => { release = resolve; });
        const transition = f.sandbox.start(async () => {
            // The worker has imported the target, but the active ID cannot be
            // switched until validation/import completes.
            f.sandbox.workerDatabase = 'new-db';
            await gate;
            f.sandbox.activeId = 'new';
            return true;
        });
        await Promise.resolve();
        f.fire(idleCallback ? f.idle : f.timers);
        assert.deepEqual(f.saves, [], 'must not save the new database under the old workspace');
        release();
        assert.equal(await transition, true);
        f.fire(f.timers);
        if (idleCallback) f.fire(f.idle);
        assert.deepEqual(f.saves, [['new', 'new-db']]);
    });
}

test('a deferred autosave is retried when a workspace transition fails', async () => {
    const f = fixture(false);
    f.sandbox.schedule(0);
    let release;
    const gate = new Promise(resolve => { release = resolve; });
    const transition = f.sandbox.start(async () => { await gate; return false; });
    await Promise.resolve();
    f.fire(f.timers);
    assert.deepEqual(f.saves, []);
    release();
    assert.equal(await transition, false);
    f.fire(f.timers);
    assert.deepEqual(f.saves, [['old', 'old-db']]);
});

test('an overlapping export cannot save the imported database under the old workspace', async () => {
    const source = fs.readFileSync(`${__dirname}/app.js`, 'utf8');
    const slice = (from, to) => {
        const start = source.indexOf(from), end = source.indexOf(to, start);
        assert.ok(start >= 0 && end > start, `missing ${from}`);
        return source.slice(start, end);
    };
    let next = 1;
    const timers = new Map(), exports = [], saves = [];
    let releaseFirstRefresh;
    const firstRefresh = new Promise(resolve => { releaseFirstRefresh = resolve; });
    let refreshCount = 0, releaseImport;
    const importing = new Promise(resolve => { releaseImport = resolve; });
    const sandbox = {
        window: {
            setTimeout(fn) { const id = next++; timers.set(id, fn); return id; },
            clearTimeout(id) { timers.delete(id); },
        },
        document: { getElementById: () => null }, console,
        showToast() {}, setWorkspaceControlsBusy() {}, hasActiveWorkspaceDataOperation: () => false,
        exportWorkspaceSnapshot() {
            return new Promise(resolve => exports.push({ data: sandbox.database, resolve }));
        },
        refreshWorkspaceSelector: async () => {
            if (++refreshCount === 1) await firstRefresh;
        },
        workspaceStore: { saveWorkspace: async (id, snapshot) => { saves.push([id, snapshot]); return {name: id}; } },
        database: 'old-v1',
    };
    vm.runInNewContext(`
        let workspaceTransition = null, queryExecutionInFlight = false, workspaceRestoreBlocked = false;
        let snapshotSaveTimer = null, snapshotIdleHandle = null, snapshotDirty = false, snapshotRevision = 0;
        let wasmReady = true, wasmApi = { exportDatabaseBytes() {} };
        let activeWorkspaceId = 'old', activeWorkspaceName = 'old', currentTables = [];
        function isWorkspaceTransitioning() { return workspaceTransition !== null; }
        ${slice('function runWorkspaceTransition(', 'function requireStableWorkspace(')}
        ${slice('async function saveDatabaseSnapshotNow(', 'async function restoreDatabaseSnapshot(')}
        globalThis.start = runWorkspaceTransition;
        globalThis.schedule = scheduleDatabaseSnapshotSave;
        globalThis.save = saveDatabaseSnapshotNow;
        globalThis.switchID = id => { activeWorkspaceId = id; };
    `, sandbox);
    const flush = async () => { for (let i = 0; i < 10; i++) await Promise.resolve(); };
    const fireTimer = () => {
        const first = timers.entries().next();
        assert.equal(first.done, false, 'expected autosave timer');
        const [id, callback] = first.value;
        timers.delete(id);
        callback();
    };
    sandbox.schedule(0);
    fireTimer(); // An earlier export is still in progress.
    assert.equal(exports.length, 1);
    sandbox.database = 'old-v2';
    sandbox.schedule(0);
    exports[0].resolve({snapshot: exports[0].data, format: 'raw'});
    await flush(); // The first save now waits on refreshing workspace metadata.
    const transition = sandbox.start(async () => {
        assert.equal(await sandbox.save(), true);
        sandbox.database = 'new-db';
        await importing;
        sandbox.switchID('new');
        return true;
    });
    await flush();
    assert.equal(exports.length, 2);
    releaseFirstRefresh(); // Marks another save pending while transition's export runs.
    await flush();
    exports[1].resolve({snapshot: exports[1].data, format: 'raw'});
    await flush();
    assert.equal(sandbox.database, 'new-db');
    assert.deepEqual(saves, [['old', 'old-v1'], ['old', 'old-v2']]);
    fireTimer(); // New database is installed, but ID is still "old".
    await flush();
    assert.equal(exports.length, 2, 'timer must not export during transition');
    releaseImport();
    assert.equal(await transition, true);
    fireTimer();
    assert.equal(exports.length, 3);
    exports[2].resolve({snapshot: exports[2].data, format: 'raw'});
    await flush();
    assert.deepEqual(saves, [['old', 'old-v1'], ['old', 'old-v2'], ['new', 'new-db']]);
});
