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

function harness(query, api, renderResults = async () => {}) {
    let saves = 0, refreshes = 0;
    const editor = { value: query, selectionStart: 0, selectionEnd: 0 };
    const results = { innerHTML: '', setAttribute() {} };
    const execute = { disabled: false, innerHTML: '' };
    const sandbox = {
        document: { getElementById: id => ({ queryEditor: editor, executeBtn: execute, resultsContainer: results })[id] },
        window: { clearVanillaGrid() {} },
        AbortController, performance, console,
        requestAnimationFrame: f => f(),
        requireStableWorkspace: () => true,
        stopLiveJobWatch() {}, showToast() {}, alert() {},
        setQueryCancellationState() {}, setOpenVanillaGridEnabled() {},
        updateStatus() {}, setRuntimeActivityHint() {},
        loadTables: async () => { refreshes++; },
        scheduleDatabaseSnapshotSave: () => { saves++; },
        refreshRuntimeStatus: async () => {},
        pushHistory() {}, saveEditorState() {}, escapeHtml: String,
        renderResults, wasmApi: api, wasmReady: true,
        queryExecutionInFlight: false, activeQueryStreamProgress: null,
        streamPreviewRenderTimer: null, activeQueryAbortController: null,
        currentResults: null, visibleResultsCache: null,
        resultViewState: {}, DEFAULT_RESULT_PAGE_SIZE: 100,
    };
    vm.runInNewContext([
        section('function sqlMayMutate(', 'function setQueryCancellationState('),
        section('async function onExecuteClick()', '// Render query results'),
        'globalThis.run = onExecuteClick;',
    ].join('\n'), sandbox);
    return { run: sandbox.run, results, counts: () => ({ saves, refreshes }) };
}

test('a failed script still saves and refreshes earlier writes', async () => {
    const h = harness('CREATE TABLE t (id INT); INSERT INTO missing VALUES (1)', {
        executeQuery() {},
        executeMulti: async () => ({ success: false, error: 'Statement 2 failed' }),
    });
    await h.run();
    assert.match(h.results.innerHTML, /Statement 2 failed/);
    assert.deepEqual(h.counts(), { saves: 1, refreshes: 1 });
});

test('an interrupted write saves and an unsuccessful read does not', async () => {
    const write = harness('INSERT INTO t VALUES (1)', {
        executeQuery() {}, executeQueryStream: async () => { throw new Error('interrupted'); },
    });
    await write.run();
    assert.deepEqual(write.counts(), { saves: 1, refreshes: 1 });
    const read = harness('SELECT 1', {
        executeQuery() {}, executeQueryStream: async () => ({ success: false, error: 'failed' }),
    });
    await read.run();
    assert.deepEqual(read.counts(), { saves: 0, refreshes: 0 });
});

test('a successful write is saved even if rendering fails', async () => {
    const h = harness('CREATE TABLE t (id INT)', {
        executeQuery() {}, executeQueryStream: async () => ({ success: true, rows: [], columns: [] }),
    }, async () => { throw new Error('render failed'); });
    await h.run();
    assert.match(h.results.innerHTML, /render failed/);
    assert.deepEqual(h.counts(), { saves: 1, refreshes: 1 });
});
